package collector

import (
	"sort"
	"time"

	"github.com/gnocent/kafkabeat/internal/kafkaclient"
	"github.com/gnocent/kafkabeat/internal/sink"
)

// GroupSource fournit les métadonnées de groupe lues dans le flux
// __consumer_offsets (voir offsetstream.Source.Groups).
type GroupSource interface {
	Groups() ([]kafkaclient.GroupView, map[string]bool, bool)
}

// Méthodes de détection des rebalances.
const (
	MethodStream   = "stream"   // enregistrement GroupMetadata, une génération par document
	MethodDescribe = "describe" // repli : changement de membres vu par DescribeGroups à chaque cycle métriques
)

// Classement d'un groupe selon ses membres (kafka.consumergroup.assignment).
const (
	AssignmentMembers = "members" // au moins un membre : assignation par le coordinateur
	AssignmentManual  = "manual"  // aucun membre, commits qui avancent (assign() : Spark, Flink...)
	AssignmentNone    = "none"    // aucun membre, commits figés
)

// États Kafka d'un groupe classique en cours de rebalance.
var rebalancingStates = map[string]bool{"PreparingRebalance": true, "CompletingRebalance": true}

// groupState suit les membres d'un groupe classique entre les cycles. Il
// n'est pas persisté : au redémarrage, la référence est reprise de
// DescribeGroups au premier cycle.
type groupState struct {
	view        kafkaclient.GroupView // dernière vue connue ; Group vide = inconnue
	fromStream  bool                  // view vient du flux
	describedAt time.Time             // dernière vue prise de DescribeGroups : le flux antérieur est déjà pris en compte
	deferred    bool                  // vu par DescribeGroups, pas encore par le flux sain : référence au cycle métriques suivant

	rebalancingSince    time.Time // premier cycle vu en rebalance depuis le dernier rebalance émis
	lastSeenRebalancing time.Time

	covered     bool                           // couverture calculée ce cycle (groupe Stable, membres consumer connus)
	unassigned  map[string]map[int32]time.Time // topic -> partition -> non assignée depuis
	lastAdvance time.Time                      // dernière avance vue d'un commit du groupe
	emptySince  time.Time                      // premier cycle vu Empty (sans membre) ; zéro sinon
}

func (c *Collector) group(name string) *groupState {
	gs := c.groups[name]
	if gs == nil {
		gs = &groupState{}
		c.groups[name] = gs
	}
	return gs
}

// trackMembership met à jour les membres des groupes et renvoie un document
// par rebalance : enregistrements GroupMetadata du flux d'abord, puis, aux
// cycles métriques, DescribeGroups. DescribeGroups fournit la référence
// initiale (au premier cycle, ou un cycle métriques après l'apparition d'un
// groupe que le flux sain n'a pas décrit) ; il ne détecte les rebalances que
// pour les groupes que le flux ne suit pas (mode polling, flux en erreur,
// métadonnées en version inconnue). Un enregistrement antérieur à la dernière
// vue de DescribeGroups est ignoré : il est déjà pris en compte. Seul un
// groupe né dans la seconde qui précède le premier cycle de l'agent peut
// perdre son rebalance de création (enregistrement encore en vol).
func (c *Collector) trackMembership(cc *cycleCtx, names []string, described map[string]kafkaclient.GroupView) []sink.Doc {
	var docs []sink.Doc
	var views []kafkaclient.GroupView
	var fallback map[string]bool
	healthy := false
	if src, ok := c.commits.(GroupSource); ok {
		views, fallback, healthy = src.Groups()
	}
	cc.method = MethodDescribe
	if healthy {
		cc.method = MethodStream
	}
	now := cc.now

	for _, v := range views {
		if !c.cfg.GroupSelected(v.Group) {
			continue
		}
		if v.Deleted {
			delete(c.groups, v.Group)
			continue
		}
		gs := c.group(v.Group)
		switch {
		case !v.At.After(gs.describedAt):
			continue
		case gs.fromStream && v.Generation < gs.view.Generation:
			continue
		case gs.fromStream && v.Generation == gs.view.Generation:
			gs.view = v // réécriture sans nouvelle génération (membre statique remplacé)
			continue
		}
		docs = append(docs, c.rebalanceDoc(cc, gs, v, MethodStream, v.At))
		gs.view, gs.fromStream = v, true
	}

	for _, g := range names {
		d, ok := described[g]
		// hors Stable et Empty, DescribeGroups ne renvoie ni abonnements ni assignations
		if !ok || (d.State != "Stable" && d.State != "Empty") {
			continue
		}
		gs := c.groups[g]
		streamOK := healthy && !fallback[g]
		switch {
		case streamOK && gs != nil && gs.view.Group != "":
			// le flux fait foi, y compris sur la référence du premier cycle :
			// un groupe né juste avant est déjà connu par son enregistrement
			continue
		case streamOK && c.membershipReady && (gs == nil || !gs.deferred):
			// groupe nouveau : son enregistrement GroupMetadata peut être en
			// vol ; une référence prise maintenant le ferait juger déjà vu et
			// perdrait le rebalance de création. Référence au cycle suivant si
			// le flux ne l'a toujours pas décrit.
			c.group(g).deferred = true
			continue
		case !c.membershipReady || streamOK:
			// référence : aucun rebalance
		case gs == nil || gs.view.Group == "":
			if len(d.Members) > 0 {
				docs = append(docs, c.rebalanceDoc(cc, c.group(g), d, MethodDescribe, now))
			}
		case membersChanged(gs.view.Members, d.Members):
			docs = append(docs, c.rebalanceDoc(cc, gs, d, MethodDescribe, now))
		}
		gs = c.group(g)
		gs.view, gs.fromStream, gs.describedAt = d, false, d.At
	}
	if described != nil {
		c.membershipReady = true
	}

	// états du groupe à ce cycle, après les rebalances émis : début et fin
	// observés du rebalance suivant, groupe sans membre
	for _, g := range names {
		gs := c.groups[g]
		if gs == nil {
			if cc.groups[g].State != "Empty" {
				continue
			}
			gs = c.group(g) // groupe sans membre ni référence (assign()) : suivi pour son classement
		}
		if cc.groups[g].State != "Empty" {
			gs.emptySince = time.Time{}
		} else if gs.emptySince.IsZero() {
			gs.emptySince = now
		}
		if rebalancingStates[cc.groups[g].State] {
			if gs.rebalancingSince.IsZero() {
				gs.rebalancingSince = now
			}
			gs.lastSeenRebalancing = now
		} else if !gs.rebalancingSince.IsZero() && now.Sub(gs.lastSeenRebalancing) > 3*c.cfg.Collect.Interval {
			gs.rebalancingSince = time.Time{} // aucun rebalance émis à la suite : observation périmée
		}
	}

	for _, g := range names {
		if gs := c.groups[g]; gs != nil {
			c.coverage(cc, g, gs)
		}
	}
	return docs
}

// coverage compare, pour un groupe consumer Stable, les partitions des topics
// auxquels ses membres sont abonnés aux partitions qui leur sont assignées,
// et date chaque partition non assignée. Hors Stable (rebalance en cours),
// les dates repartent de zéro.
func (c *Collector) coverage(cc *cycleCtx, g string, gs *groupState) {
	gs.covered = false
	if cc.groups[g].State != "Stable" || gs.view.ProtocolType != "consumer" || len(gs.view.Members) == 0 {
		gs.unassigned = nil
		return
	}
	assigned := make(map[string]map[int32]bool)
	for _, m := range gs.view.Members {
		for t, parts := range m.Assigned {
			if assigned[t] == nil {
				assigned[t] = make(map[int32]bool)
			}
			for _, p := range parts {
				assigned[t][p] = true
			}
		}
	}
	next := make(map[string]map[int32]time.Time)
	for _, t := range gs.subscribed() {
		tm, ok := cc.md.Topics[t]
		if !ok || tm.Err != nil || tm.Internal || !c.cfg.TopicSelected(t) {
			continue
		}
		for _, p := range tm.Partitions {
			if assigned[t][p.ID] {
				continue
			}
			since, ok := gs.unassigned[t][p.ID]
			if !ok {
				since = cc.now
			}
			if next[t] == nil {
				next[t] = make(map[int32]time.Time)
			}
			next[t][p.ID] = since
		}
	}
	gs.unassigned, gs.covered = next, true
}

// subscribed renvoie les topics auxquels au moins un membre est abonné, triés.
func (gs *groupState) subscribed() []string {
	set := make(map[string]bool)
	for _, m := range gs.view.Members {
		for _, t := range m.Topics {
			set[t] = true
		}
	}
	return sortedKeys(set)
}

// classify renvoie le nombre de membres d'un groupe et son classement (vide
// pendant les deux cycles qui suivent le départ du dernier membre) ; ok est
// faux quand les membres sont inconnus (groupe KIP-848, DescribeGroups en
// échec).
func (c *Collector) classify(cc *cycleCtx, g string) (members int, assignment string, ok bool) {
	gs := c.groups[g]
	switch {
	case cc.groups[g].State == "Empty":
		members = 0
	case gs != nil && gs.view.Group != "":
		members = len(gs.view.Members)
	default:
		return 0, "", false
	}
	switch {
	case members > 0:
		assignment = AssignmentMembers
	case gs == nil || gs.emptySince.IsZero() || cc.now.Sub(gs.emptySince) < 2*c.cfg.Collect.Interval:
		// sans membre depuis moins de deux cycles : manual et none pas encore
		// départagés, classement absent
	// commits qui avancent sans membre : vus plus d'un cycle après le départ du
	// dernier membre (son dernier commit n'est vu qu'au cycle suivant)
	case gs.lastAdvance.After(gs.emptySince.Add(c.cfg.Collect.Interval)) && cc.now.Sub(gs.lastAdvance) < c.cfg.Collect.StalledAfter:
		assignment = AssignmentManual
	default:
		assignment = AssignmentNone
	}
	return members, assignment, true
}

// forgetGroups oublie les groupes qui ne sont plus listés, sauf si la liste
// est incomplète (un groupe absent n'a pas forcément disparu).
func (c *Collector) forgetGroups(cc *cycleCtx) {
	if cc.groupsPartial {
		return
	}
	for g := range c.groups {
		if _, ok := cc.groups[g]; !ok {
			delete(c.groups, g)
		}
	}
}

func membersChanged(a, b []kafkaclient.Member) bool {
	if len(a) != len(b) {
		return true
	}
	ids := make(map[string]bool, len(a))
	for _, m := range a {
		ids[m.ID] = true
	}
	for _, m := range b {
		if !ids[m.ID] {
			return true
		}
	}
	return false
}

// rebalanceDoc décrit le passage de gs.view à v : membres entrés et sortis,
// génération (flux), durée observée par les cycles (minorant, absente si
// aucun cycle n'a vu le groupe en rebalance).
func (c *Collector) rebalanceDoc(cc *cycleCtx, gs *groupState, v kafkaclient.GroupView, method string, at time.Time) sink.Doc {
	before := make(map[string]bool, len(gs.view.Members))
	for _, m := range gs.view.Members {
		before[m.ID] = true
	}
	after := make(map[string]bool, len(v.Members))
	var joined, left []map[string]any
	for _, m := range v.Members {
		after[m.ID] = true
		if !before[m.ID] {
			joined = append(joined, memberField(m))
		}
	}
	for _, m := range gs.view.Members {
		if !after[m.ID] {
			left = append(left, memberField(m))
		}
	}
	rb := map[string]any{
		"method":         method,
		"members_joined": len(joined),
		"members_left":   len(left),
	}
	if len(joined) > 0 {
		rb["joined"] = joined
	}
	if len(left) > 0 {
		rb["left"] = left
	}
	if gs.fromStream && method == MethodStream {
		rb["previous_generation"] = gs.view.Generation
	}
	if !gs.rebalancingSince.IsZero() && !gs.rebalancingSince.After(at) {
		end := at
		if method == MethodDescribe {
			end = gs.lastSeenRebalancing
		}
		rb["duration_s"] = seconds(end.Sub(gs.rebalancingSince))
		gs.rebalancingSince = time.Time{}
	}
	group := map[string]any{
		"id":            v.Group,
		"members":       len(v.Members),
		"protocol_type": v.ProtocolType,
		"protocol":      v.Protocol,
	}
	if v.Generation >= 0 {
		group["generation"] = v.Generation
	}
	kafka := map[string]any{
		"cluster":       map[string]any{"name": c.cfg.Cluster, "id": cc.md.ClusterID},
		"consumergroup": group,
		"rebalance":     rb,
	}
	// topics abonnés et leurs propriétaires : filtres topic et équipe des dashboards
	topics := (&groupState{view: v}).subscribed()
	if len(topics) > 0 {
		owners := make(map[string]bool)
		for _, t := range topics {
			if o := c.cfg.TopicOwner(t); o != "" {
				owners[o] = true
			}
		}
		topic := map[string]any{"name": topics}
		if len(owners) > 0 {
			topic["owner"] = sortedKeys(owners)
		}
		kafka["topic"] = topic
	}
	body := map[string]any{
		"@timestamp": at.UTC().Format(time.RFC3339Nano),
		"event":      map[string]any{"dataset": "kafkabeat.rebalance", "module": "kafkabeat", "kind": "event"},
		"kafka":      kafka,
	}
	c.enrich(body)
	return sink.Doc{Stream: "events", Body: body}
}

func memberField(m kafkaclient.Member) map[string]any {
	f := map[string]any{"member_id": m.ID, "client_id": m.ClientID, "client_host": m.ClientHost}
	if m.InstanceID != "" {
		f["instance_id"] = m.InstanceID
	}
	return f
}

// assignedMembers renvoie le membre assigné à chaque partition (client.* des
// documents consumergroup, comme Metricbeat).
func assignedMembers(described map[string]kafkaclient.GroupView) map[string]map[string]map[int32]kafkaclient.Member {
	out := make(map[string]map[string]map[int32]kafkaclient.Member)
	for g, v := range described {
		for _, m := range v.Members {
			for t, parts := range m.Assigned {
				for _, p := range parts {
					if out[g] == nil {
						out[g] = make(map[string]map[int32]kafkaclient.Member)
					}
					if out[g][t] == nil {
						out[g][t] = make(map[int32]kafkaclient.Member)
					}
					out[g][t][p] = m
				}
			}
		}
	}
	return out
}

// unassignedOf résume la couverture d'un topic pour un groupe : partitions
// non assignées (triées), la plus ancienne non-assignation, et ok = couverture
// connue ce cycle.
func (gs *groupState) unassignedOf(topic string, now time.Time) (ids []int32, oldest time.Duration, ok bool) {
	if gs == nil || !gs.covered {
		return nil, 0, false
	}
	for p, since := range gs.unassigned[topic] {
		ids = append(ids, p)
		if d := now.Sub(since); d > oldest {
			oldest = d
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, oldest, true
}
