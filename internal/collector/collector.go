// Package collector exécute le cycle de collecte : inventaire, offsets de fin,
// offsets commités, encadrement du lag, états, flags, taux et documents.
package collector

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/gnocent/kafkabeat/internal/config"
	"github.com/gnocent/kafkabeat/internal/history"
	"github.com/gnocent/kafkabeat/internal/kafkaclient"
	"github.com/gnocent/kafkabeat/internal/sink"
)

// Broker est la partie du client Kafka utilisée par le collecteur.
type Broker interface {
	Metadata(ctx context.Context) (kafkaclient.Metadata, error)
	EndOffsets(ctx context.Context, topics []string) (kafkaclient.Offsets, []error, error)
	StartOffsets(ctx context.Context, topics []string) (kafkaclient.Offsets, []error, error)
	ListGroups(ctx context.Context) (map[string]kafkaclient.Group, []error, error)
	GroupsUnavailable(ctx context.Context, groups []string) map[string]error
	Members(ctx context.Context, groups []string) (map[string]kafkaclient.GroupView, error)
}

// CommitSource fournit les offsets commités (polling, ou flux + polling).
type CommitSource interface {
	Commits(ctx context.Context, groups []string) (kafkaclient.GroupCommits, map[string]error, error)
	Mode() string
}

// États d'un couple topic + groupe.
const (
	StateCaughtUp = "caught_up"
	StateLagging  = "lagging"
	StateStalled  = "stalled"
	StateNoCommit = "no_commit"
	StateGone     = "gone"
	// au moins une partition d'un topic auquel le groupe est abonné n'a aucun
	// membre assigné depuis stalled_after, hors rebalance ; prime sur stalled
	StateUnassigned = "unassigned"
)

// Statuts de production d'un topic, portés par chacune de ses paires.
const (
	ProductionActive  = "active"
	ProductionStopped = "stopped"
)

// Collector porte l'état entre les cycles.
type Collector struct {
	cfg     config.Config
	broker  Broker
	commits CommitSource
	hist    *history.Store
	out     sink.Sink
	logger  *log.Logger
	version string

	cycle        uint64
	lastMetrics  time.Time
	lastSnapshot time.Time
	pairs        map[pairKey]*pairState
	topics       map[string]*topicState
	logStart     kafkaclient.Offsets // dernier log start lu (cycles métriques), pour le risque de perte à chaque cycle

	groups          map[string]*groupState
	membershipReady bool // référence DescribeGroups prise

	jolokia HealthSource
}

// HealthSource complète le document de santé : sa section et ses erreurs.
type HealthSource interface {
	Health() (map[string]any, []string)
}

// SetJolokia rattache le metricset broker au document de santé.
func (c *Collector) SetJolokia(h HealthSource) { c.jolokia = h }

type pairKey struct {
	Group string
	Topic string
}

// HistoryTiers convertit les paliers de la configuration.
func HistoryTiers(h config.History) []history.Tier {
	out := make([]history.Tier, len(h.Tiers))
	for i, t := range h.Tiers {
		out[i] = history.Tier{Step: t.Step, Span: t.Span}
	}
	return out
}

// New crée un collecteur.
func New(cfg config.Config, broker Broker, commits CommitSource, hist *history.Store, out sink.Sink, logger *log.Logger, version string) *Collector {
	return &Collector{
		cfg: cfg, broker: broker, commits: commits, hist: hist, out: out, logger: logger, version: version,
		pairs:    make(map[pairKey]*pairState),
		topics:   make(map[string]*topicState),
		groups:   make(map[string]*groupState),
		logStart: make(kafkaclient.Offsets),
	}
}

// topicField construit l'objet kafka.topic, avec le propriétaire quand une
// règle correspond ; id vide = absent (documents aux noms Metricbeat).
func (c *Collector) topicField(name, id string) map[string]any {
	t := map[string]any{"name": name}
	if id != "" {
		t["id"] = id
	}
	if owner := c.cfg.TopicOwner(name); owner != "" {
		t["owner"] = owner
	}
	return t
}

// Run enchaîne les cycles sur la grille de l'intervalle jusqu'à annulation,
// avec un snapshot périodique et un snapshot final.
func (c *Collector) Run(ctx context.Context) error {
	interval := c.cfg.Collect.Interval
	for {
		now := time.Now()
		c.Cycle(ctx, now)
		if now.Sub(c.lastSnapshot) >= c.cfg.State.SnapshotInterval {
			c.snapshot(now)
		}
		next := now.Truncate(interval).Add(interval)
		select {
		case <-ctx.Done():
			c.snapshot(time.Now())
			return ctx.Err()
		case <-time.After(time.Until(next)):
		}
	}
}

func (c *Collector) snapshot(now time.Time) {
	if err := c.Save(c.cfg.State.Path, now); err != nil {
		c.logger.Printf("snapshot: %v", err)
	}
}

type healthInfo struct {
	errors        []string
	partialErrors int
	groupErrors   int
	topics        int
	partitions    int
	groups        int
	pairs         int
	pairsGone     int
	pairsUnknown  int
	docs          int
	incomplete    bool
	failed        bool // cycle abandonné (Kafka injoignable) : seul le document de santé part
}

// cycleCtx regroupe ce qui est connu pendant un cycle.
type cycleCtx struct {
	parent        context.Context // contexte de l'appelant : annulé = arrêt de l'agent, pas une panne
	now           time.Time
	ts            time.Time
	md            kafkaclient.Metadata
	leo           kafkaclient.Offsets
	truncated     map[string]bool // topics dont au moins une partition a reculé
	recreated     map[string]bool // topics dont l'identifiant a changé
	groups        map[string]kafkaclient.Group
	groupsPartial bool // un broker n'a pas listé ses groupes : un groupe absent n'a pas forcément disparu
	groupErrs     map[string]error
	method        string           // détection des rebalances à ce cycle (stream ou describe)
	lagGroups     []map[string]any // kafka.consumergroup des documents de lag, complété après la boucle des paires
	h             *healthInfo

	// cycle métriques (documents aux noms Metricbeat)
	metrics bool
	start   kafkaclient.Offsets
	brokers map[int32]string
	members map[string]map[string]map[int32]kafkaclient.Member // groupe -> topic -> partition -> membre assigné
	extra   []sink.Doc
}

// Cycle exécute un cycle complet à l'instant now et publie les documents.
// Un cycle ne peut pas durer plus de trois intervalles : au-delà il est
// abandonné et signalé incomplet.
func (c *Collector) Cycle(ctx context.Context, now time.Time) {
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, 3*c.cfg.Collect.Interval)
	defer cancel()
	start := time.Now()
	c.cycle++
	cc := &cycleCtx{parent: parent, now: now, ts: now.Truncate(c.cfg.Collect.Interval), h: &healthInfo{},
		truncated: make(map[string]bool), recreated: make(map[string]bool)}
	var docs []sink.Doc

	md, err := c.broker.Metadata(ctx)
	if err != nil {
		c.fail(cc, start, fmt.Errorf("metadata: %w", err))
		return
	}
	cc.md = md
	topics := c.selectedTopics(md)
	cc.h.topics = len(topics)
	c.trackTopics(cc, topics)

	leo, partial, err := c.broker.EndOffsets(ctx, topics)
	if err != nil {
		c.fail(cc, start, fmt.Errorf("end offsets: %w", err))
		return
	}
	cc.leo = leo
	cc.h.partialErrors += len(partial)
	cc.h.incomplete = len(partial) > 0
	for t, parts := range leo {
		for p, off := range parts {
			if c.hist.Observe(history.Key{Topic: t, Partition: p}, off, now) {
				cc.truncated[t] = true
				delete(c.logStart, t)
			}
			cc.h.partitions++
		}
	}

	cc.metrics = now.Sub(c.lastMetrics) >= c.cfg.Collect.MetricsInterval
	if cc.metrics {
		cc.brokers = make(map[int32]string, len(md.Brokers))
		for _, b := range md.Brokers {
			cc.brokers[b.ID] = net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port))) // IPv6 littérale encadrée
		}
		start, partial, err := c.broker.StartOffsets(ctx, topics)
		if err != nil {
			cc.h.errors = append(cc.h.errors, fmt.Sprintf("start offsets: %v", err))
			cc.h.incomplete = true
		} else {
			cc.start = start
			cc.h.partialErrors += len(partial)
			// une partition en échec garde son dernier log start : il ne fait qu'avancer
			for t, parts := range start {
				if c.logStart[t] == nil {
					c.logStart[t] = make(map[int32]int64, len(parts))
				}
				for p, off := range parts {
					c.logStart[t][p] = off
				}
			}
		}
		docs = append(docs, c.partitionDocs(cc, topics)...)
	}

	groups, partial, err := c.broker.ListGroups(ctx)
	if err != nil {
		c.fail(cc, start, fmt.Errorf("groups: %w", err))
		return
	}
	cc.groups = groups
	if len(partial) > 0 {
		// les paires des groupes non listés restent inconnues ce cycle (ni
		// émises ni purgées) et aucun groupe n'est oublié
		cc.groupsPartial = true
		cc.h.partialErrors += len(partial)
		cc.h.incomplete = true
		cc.h.errors = append(cc.h.errors, fmt.Sprintf("groups: %v", errors.Join(partial...)))
	}
	// un groupe connu absent de la liste complète : supprimé, ou coordinateur
	// en cours de chargement (reprise d'un broker), que ListGroups omet sans
	// erreur ; DescribeGroups départage, et un groupe muet reste inconnu
	var unavailable map[string]error
	if missing := c.unlistedGroups(groups); !cc.groupsPartial && len(missing) > 0 {
		unavailable = c.broker.GroupsUnavailable(ctx, missing)
	}
	names := make([]string, 0, len(groups))
	for g := range groups {
		if c.cfg.GroupSelected(g) {
			names = append(names, g)
		}
	}
	sort.Strings(names)
	cc.h.groups = len(names)

	commits, groupErrs, err := c.commits.Commits(ctx, names)
	if err != nil {
		c.fail(cc, start, fmt.Errorf("committed offsets: %w", err))
		return
	}
	cc.groupErrs = make(map[string]error, len(groupErrs)+len(unavailable))
	for g, e := range groupErrs {
		cc.groupErrs[g] = e
	}
	for g, e := range unavailable {
		cc.groupErrs[g] = e
		cc.h.errors = append(cc.h.errors, fmt.Sprintf("group %s: %v", g, e))
	}
	cc.h.groupErrors = len(cc.groupErrs)
	if len(cc.groupErrs) > 0 {
		cc.h.incomplete = true
	}
	var described map[string]kafkaclient.GroupView
	if cc.metrics {
		// membres des groupes (client.* des documents consumergroup, référence
		// et repli des rebalances) : un échec n'invalide pas le cycle, les
		// documents partent sans client
		described, err = c.broker.Members(ctx, names)
		if err != nil {
			cc.h.errors = append(cc.h.errors, fmt.Sprintf("group members: %v", err))
		}
		cc.members = assignedMembers(described)
	}
	docs = append(docs, c.trackMembership(cc, names, described)...)

	// paires : commits, et abonnements des membres (paire sans commit)
	pairTopics := make(map[string]map[string]bool)
	add := func(g, t string) {
		if pairTopics[g] == nil {
			pairTopics[g] = make(map[string]bool)
		}
		pairTopics[g][t] = true
	}
	for g, topics := range commits {
		if c.cfg.GroupSelected(g) {
			for t := range topics {
				add(g, t)
			}
		}
	}
	for _, g := range names {
		if gs := c.groups[g]; gs != nil {
			for _, t := range gs.subscribed() {
				add(g, t)
			}
		}
	}
	seen := make(map[pairKey]bool)
	for _, g := range sortedKeys(pairTopics) {
		for _, t := range sortedKeys(pairTopics[g]) {
			tm, ok := md.Topics[t]
			// les topics internes n'ont que des documents partition
			if !ok || tm.Err != nil || tm.Internal || !c.cfg.TopicSelected(t) {
				continue
			}
			key := pairKey{Group: g, Topic: t}
			seen[key] = true
			docs = append(docs, c.lagDoc(cc, key, tm, commits[g][t]))
			cc.h.pairs++
		}
	}
	// membres et classement du groupe, connus une fois tous ses commits vus
	for _, cg := range cc.lagGroups {
		if n, a, ok := c.classify(cc, cg["id"].(string)); ok {
			cg["members"] = n
			if a != "" {
				cg["assignment"] = a
			}
		}
	}
	docs = append(docs, c.missingPairs(cc, seen)...)
	docs = append(docs, cc.extra...)

	if cc.metrics {
		c.lastMetrics = now
		c.hist.Compact(now)
		c.forgetTopics(md)
		c.forgetGroups(cc)
	}

	cc.h.docs = len(docs)
	if err := c.out.Emit(docs); err != nil {
		c.logger.Printf("cycle %d: sending documents: %v", c.cycle, err)
	}
	// document de santé envoyé après les autres : sa durée de cycle inclut
	// leur émission, qui peut dépasser l'intervalle quand Elasticsearch peine
	if err := c.out.Emit([]sink.Doc{c.healthDoc(cc, start)}); err != nil {
		c.logger.Printf("cycle %d: sending health document: %v", c.cycle, err)
	}
}

// fail abandonne le cycle : seul le document de santé part, avec l'erreur.
// Un arrêt de l'agent en cours de cycle n'est pas une panne : rien n'est publié.
func (c *Collector) fail(cc *cycleCtx, start time.Time, err error) {
	if cc.parent != nil && cc.parent.Err() != nil {
		c.logger.Printf("cycle %d interrupted by shutdown", c.cycle)
		return
	}
	c.logger.Printf("cycle %d: %v", c.cycle, err)
	cc.h.errors = append(cc.h.errors, err.Error())
	cc.h.incomplete = true
	cc.h.failed = true
	if e := c.out.Emit([]sink.Doc{c.healthDoc(cc, start)}); e != nil {
		c.logger.Printf("cycle %d: sending health document: %v", c.cycle, e)
	}
}

// unlistedGroups renvoie les groupes des paires connues qui manquent à la
// liste de ce cycle.
func (c *Collector) unlistedGroups(listed map[string]kafkaclient.Group) []string {
	set := make(map[string]bool)
	for key := range c.pairs {
		if _, ok := listed[key.Group]; !ok && c.cfg.GroupSelected(key.Group) {
			set[key.Group] = true
		}
	}
	return sortedKeys(set)
}

func (c *Collector) selectedTopics(md kafkaclient.Metadata) []string {
	topics := make([]string, 0, len(md.Topics))
	for name, t := range md.Topics {
		if t.Err != nil || !c.cfg.TopicSelected(name) {
			continue
		}
		topics = append(topics, name)
	}
	sort.Strings(topics)
	return topics
}

// trackTopics détecte les topics recréés (identifiant changé) et oublie
// leur historique, qui ne décrit plus le journal. Le flag est posé sur toutes
// les paires connues du topic : Kafka efface les offsets d'un topic supprimé,
// si bien qu'au cycle du nouvel identifiant la paire est souvent absente
// (document gone) et ne revient qu'au commit suivant.
func (c *Collector) trackTopics(cc *cycleCtx, topics []string) {
	for _, name := range topics {
		tm := cc.md.Topics[name]
		ts := c.topics[name]
		if ts == nil {
			c.topics[name] = &topicState{ID: tm.ID}
			continue
		}
		if tm.ID != "" && ts.ID != "" && tm.ID != ts.ID {
			cc.recreated[name] = true
			delete(c.logStart, name)
			for _, p := range tm.Partitions {
				c.hist.Forget(history.Key{Topic: name, Partition: p.ID})
			}
			for key, st := range c.pairs {
				if key.Topic == name {
					st.raise(flagTopicRecr, c.cfg.Collect.FlagHoldCycles)
				}
			}
		}
		ts.ID = tm.ID
	}
}

// forgetTopics purge l'historique des topics disparus de l'inventaire.
// L'identifiant d'un topic absent est gardé tant qu'une paire le connaît :
// un cycle métriques entre suppression et recréation ne doit pas masquer
// la recréation.
func (c *Collector) forgetTopics(md kafkaclient.Metadata) {
	for _, k := range c.hist.Keys() {
		if _, ok := md.Topics[k.Topic]; !ok {
			c.hist.Forget(k)
		}
	}
	for name := range c.logStart {
		if _, ok := md.Topics[name]; !ok {
			delete(c.logStart, name)
		}
	}
	known := make(map[string]bool, len(c.pairs))
	for key := range c.pairs {
		known[key.Topic] = true
	}
	for name := range c.topics {
		if _, ok := md.Topics[name]; !ok && !known[name] {
			delete(c.topics, name)
		}
	}
}

// missingPairs traite les paires connues non vues ce cycle : état gone
// pendant GoneCycles puis purge, sauf si la collecte de leur groupe ou de
// leur topic a échoué (inconnu ce cycle : état conservé, rien n'est émis).
func (c *Collector) missingPairs(cc *cycleCtx, seen map[pairKey]bool) []sink.Doc {
	var docs []sink.Doc
	for _, key := range sortedPairKeys(c.pairs) {
		if seen[key] {
			continue
		}
		st := c.pairs[key]
		tm, topicKnown := cc.md.Topics[key.Topic]
		_, groupListed := cc.groups[key.Group]
		if cc.groupErrs[key.Group] != nil || (topicKnown && tm.Err != nil) || (cc.groupsPartial && !groupListed) {
			cc.h.pairsUnknown++
			continue
		}
		if st.Gone == 0 {
			st.Gone = c.cfg.Collect.GoneCycles
			st.resetReferences()
		}
		docs = append(docs, c.goneDoc(cc, key, tm.ID, len(tm.Partitions)))
		cc.h.pairsGone++
		st.Gone--
		c.decayFlags(st)
		if st.Gone == 0 {
			delete(c.pairs, key)
		}
	}
	return docs
}

type partView struct {
	id      int32
	leo     int64
	hasLeo  bool
	commit  kafkaclient.Commit
	hasCom  bool
	lagOff  int64
	bracket history.Bracket
	stalled bool
	reset   bool
	age     time.Duration
	unknown bool

	retention    float64 // part du journal retenu non lue (voir retention)
	hasRetention bool
	lost         int64 // offsets passés sous le log start avant d'être lus
}

func (c *Collector) lagDoc(cc *cycleCtx, key pairKey, tm kafkaclient.TopicMeta, commits map[int32]kafkaclient.Commit) sink.Doc {
	now := cc.now
	st := c.pairs[key]
	if st == nil {
		st = &pairState{Parts: make(map[int32]*partCommit)}
		c.pairs[key] = st
	} else if st.Gone > 0 {
		st.Gone = 0
		st.raise(flagGroupRecr, c.cfg.Collect.FlagHoldCycles)
		st.resetReferences()
	}
	hold := c.cfg.Collect.FlagHoldCycles
	rewind := false
	if cc.truncated[key.Topic] {
		st.raise(flagLogTruncated, hold)
		rewind = true
	}
	if cc.recreated[key.Topic] {
		rewind = true // flag déjà posé par trackTopics
	}

	views := make([]partView, 0, len(tm.Partitions))
	advanced := false   // un commit du groupe a avancé ce cycle (classement manual)
	leoMissing := false // une partition sans offset de fin : totaux incomplets, aucun taux
	var produced, consumed int64
	var lastProduced time.Time // dernière avance d'un LEO du topic, ou début de l'observation
	producing := false         // une avance vue depuis moins de stalled_after
	for _, p := range tm.Partitions {
		v := partView{id: p.ID}
		v.leo, v.hasLeo = cc.leo.Get(tm.Name, p.ID)
		v.commit, v.hasCom = commits[p.ID]
		if v.hasLeo {
			produced += v.leo
			if at, changed, ok := c.hist.LastChange(history.Key{Topic: tm.Name, Partition: p.ID}); ok {
				if at.After(lastProduced) {
					lastProduced = at
				}
				producing = producing || changed && now.Sub(at) < c.cfg.Collect.StalledAfter
			}
		} else {
			v.unknown, leoMissing = true, true
		}
		if !v.hasCom {
			views = append(views, v)
			continue
		}
		consumed += v.commit.Offset
		pc := st.Parts[p.ID]
		// heure d'un changement d'offset : celle du commit quand le flux la
		// connaît (plus précise que le cycle), sinon maintenant
		changedAt := now
		if !v.commit.At.IsZero() {
			changedAt = v.commit.At
		}
		switch {
		case pc == nil:
			pc = &partCommit{Offset: v.commit.Offset, ChangedAt: changedAt}
			st.Parts[p.ID] = pc
		case v.commit.Offset < pc.Offset:
			v.reset = true
			pc.Offset, pc.ChangedAt = v.commit.Offset, changedAt
		case v.commit.Offset > pc.Offset:
			advanced = true
			pc.Offset, pc.ChangedAt = v.commit.Offset, changedAt
		case !v.commit.At.IsZero() && v.commit.At.After(pc.CommittedAt):
			// nouveau commit au même offset (flux) : le consumer vit sans
			// avancer ; stalled n'en dépend pas, le classement manual si
			advanced = true
		}
		pc.CommittedAt = v.commit.At
		if v.reset {
			st.raise(flagOffsetReset, hold)
			rewind = true
		}
		if !v.hasLeo {
			views = append(views, v)
			continue
		}
		v.lagOff = v.leo - v.commit.Offset
		if v.lagOff < 0 {
			v.lagOff = 0
		}
		v.retention, v.lost, v.hasRetention = c.retention(tm.Name, p.ID, v.leo, v.commit.Offset)
		if v.lagOff > 0 {
			// stalled : offset figé depuis stalled_after, que le consumer
			// recommite ou non ; l'âge publié est celui du dernier commit
			// quand le flux le date, sinon celui du dernier changement d'offset
			frozen := now.Sub(pc.ChangedAt)
			v.stalled = frozen >= c.cfg.Collect.StalledAfter
			v.age = frozen
			if !pc.CommittedAt.IsZero() {
				v.age = now.Sub(pc.CommittedAt)
			}
			v.bracket = c.hist.Bracket(history.Key{Topic: tm.Name, Partition: p.ID}, v.commit.Offset, now)
		}
		views = append(views, v)
		if cc.metrics {
			cc.extra = append(cc.extra, c.consumerGroupDoc(cc, key, tm, v))
		}
	}
	for id := range st.Parts {
		if _, ok := commits[id]; !ok {
			delete(st.Parts, id)
		}
	}

	// Taux : références du cycle précédent, remises à zéro après un retour arrière.
	if rewind {
		st.resetReferences()
	}
	var produceRate, consumeRate, smoothed any
	var intervalMs any
	// une partition sans offset de fin (erreur partielle) fausserait les deltas
	// (débit négatif puis rebond) : aucun taux ce cycle, et les références
	// restent celles du dernier cycle complet
	if !st.LastAt.IsZero() && !leoMissing {
		dt := now.Sub(st.LastAt)
		if dt > 0 {
			intervalMs = dt.Milliseconds()
			produceRate = rate(produced-st.LastProduced, dt)
			consumeRate = rate(consumed-st.LastConsumed, dt)
			st.Window = append(st.Window, rateSample{Consumed: consumed - st.LastConsumed, Dt: dt})
			// échantillons les plus récents qui tiennent dans la fenêtre (le
			// dernier toujours) : first est l'indice du plus ancien conservé
			var sum int64
			var span time.Duration
			first := len(st.Window)
			for first > 0 {
				s := st.Window[first-1]
				if span+s.Dt > c.cfg.Collect.SmoothingWindow && span > 0 {
					break
				}
				span += s.Dt
				sum += s.Consumed
				first--
			}
			st.Window = append(st.Window[:0], st.Window[first:]...)
			smoothed = rate(sum, span)
		}
	}
	if !leoMissing {
		st.LastAt, st.LastProduced, st.LastConsumed = now, produced, consumed
	}

	// Agrégats.
	var (
		offsetsSum                                  int64
		lowerMax                                    time.Duration
		upperMax                                    time.Duration
		upperKnown                                                = true
		worstTime                                   int32         = -1
		worstTimeScore                              time.Duration = -1
		worstOffsets                                int32         = -1
		worstOffsetsLag                             int64         = -1
		lagging, noCommit, beyond, stalled, unknown int
		commitAgeMax                                time.Duration
		details                                     []map[string]any
		retentionMax                                float64
		retentionKnown                              bool
		lostSum                                     int64
	)
	for _, v := range views {
		switch {
		case v.unknown:
			unknown++
			continue
		case !v.hasCom:
			noCommit++
			details = append(details, map[string]any{"id": v.id, "no_commit": true, "offset_newest": v.leo})
			continue
		}
		offsetsSum += v.lagOff
		if v.lagOff > worstOffsetsLag {
			worstOffsetsLag, worstOffsets = v.lagOff, v.id
		}
		if v.hasRetention {
			retentionKnown = true
			retentionMax = math.Max(retentionMax, v.retention)
			lostSum += v.lost
		}
		if v.lagOff == 0 {
			continue
		}
		lagging++
		if v.stalled {
			stalled++
		}
		if v.age > commitAgeMax {
			commitAgeMax = v.age
		}
		d := map[string]any{
			"id":                v.id,
			"lag_offsets":       v.lagOff,
			"offset_newest":     v.leo,
			"offset_committed":  v.commit.Offset,
			"seconds_lower":     seconds(v.bracket.Lower),
			"method":            v.bracket.Method(),
			"last_commit_age_s": seconds(v.age),
			"stalled":           v.stalled,
		}
		if v.reset {
			d[flagOffsetReset] = true
		}
		if v.hasRetention {
			d["retention_used"] = v.retention
		}
		if v.lost > 0 {
			d["offsets_lost"] = v.lost
		}
		if v.bracket.Lower > lowerMax {
			lowerMax = v.bracket.Lower
		}
		score := v.bracket.Upper
		if v.bracket.UpperKnown {
			d["seconds_upper"] = seconds(v.bracket.Upper)
			if v.bracket.Upper > upperMax {
				upperMax = v.bracket.Upper
			}
		} else {
			beyond++
			upperKnown = false
			score = v.bracket.Lower
		}
		if score > worstTimeScore {
			worstTimeScore, worstTime = score, v.id
		}
		details = append(details, d)
	}

	if advanced {
		c.group(key.Group).lastAdvance = now
	}
	if lostSum > 0 {
		st.raise(flagLogStartPassed, hold)
	}
	unassigned, unassignedAge, covered := c.groups[key.Group].unassignedOf(tm.Name, now)
	state := StateCaughtUp
	switch {
	case covered && len(unassigned) > 0 && unassignedAge >= c.cfg.Collect.StalledAfter:
		state = StateUnassigned
	case stalled > 0:
		state = StateStalled
	case lagging > 0:
		state = StateLagging
	case noCommit > 0:
		state = StateNoCommit
	}

	lag := map[string]any{
		"state":                     state,
		"offsets_sum":               offsetsSum,
		"seconds_lower":             seconds(lowerMax),
		"method":                    "bracket",
		"partition_count":           len(tm.Partitions),
		"partitions_lagging":        lagging,
		"partitions_stalled":        stalled,
		"partitions_no_commit":      noCommit,
		"partitions_beyond_history": beyond,
		"partitions_unknown":        unknown,
		"worst_partition_time":      worstTime,
		"worst_partition_offsets":   worstOffsets,
		"last_commit_age_max_s":     seconds(commitAgeMax),
		"produced_total":            produced,
		"consumed_total":            consumed,
		"produce_rate":              produceRate,
		"consume_rate":              consumeRate,
		"consume_rate_smoothed":     smoothed,
		"smoothing_window_s":        c.cfg.Collect.SmoothingWindow.Seconds(),
		"interval_ms":               intervalMs,
		"flags":                     c.flags(st, cc.h.incomplete),
	}
	if upperKnown {
		lag["seconds_upper"] = seconds(upperMax)
	} else {
		lag["seconds_upper"] = nil
		lag["method"] = "bracket_open"
	}
	if len(details) > 0 {
		lag["partitions"] = details
	}
	if retentionKnown {
		lag["retention_used"] = retentionMax
	}
	if lostSum > 0 {
		lag["offsets_lost"] = lostSum
	}
	if covered {
		lag["partitions_unassigned"] = len(unassigned)
		if len(unassigned) > 0 {
			lag["partitions_unassigned_ids"] = unassigned
			lag["unassigned_age_max_s"] = seconds(unassignedAge)
		}
	}
	// Production du topic : arrêtée dès qu'aucun message n'est vu depuis
	// stalled_after ; active seulement si une avance a été vue (au démarrage,
	// l'âge n'est qu'un minorant et le statut reste inconnu).
	if !lastProduced.IsZero() {
		age := now.Sub(lastProduced)
		lag["last_produce_age_s"] = seconds(age)
		switch {
		case age >= c.cfg.Collect.StalledAfter:
			lag["production"] = ProductionStopped
		case producing:
			lag["production"] = ProductionActive
		}
	}
	c.decayFlags(st)
	group := groupField(cc, key.Group)
	cc.lagGroups = append(cc.lagGroups, group)
	body := map[string]any{
		"@timestamp": cc.ts.UTC().Format(time.RFC3339Nano),
		"event":      map[string]any{"dataset": "kafkabeat.lag", "module": "kafkabeat"},
		"kafka": map[string]any{
			"cluster":       map[string]any{"name": c.cfg.Cluster, "id": cc.md.ClusterID},
			"topic":         c.topicField(tm.Name, tm.ID),
			"consumergroup": group,
			"lag":           lag,
		},
	}
	c.enrich(body)
	return sink.Doc{Stream: "lag", Body: body}
}

// partitionDocs produit, comme le metricset partition de Metricbeat, un
// document par partition et par réplica.
func (c *Collector) partitionDocs(cc *cycleCtx, topics []string) []sink.Doc {
	var docs []sink.Doc
	period := c.cfg.Collect.MetricsInterval.Milliseconds()
	for _, name := range topics {
		tm := cc.md.Topics[name]
		for _, p := range tm.Partitions {
			newest, hasNewest := cc.leo.Get(name, p.ID)
			if !hasNewest {
				continue
			}
			replicas := p.Replicas
			if len(replicas) == 0 {
				replicas = []int32{p.Leader}
			}
			for _, r := range replicas {
				offset := map[string]any{"newest": newest}
				if oldest, ok := cc.start.Get(name, p.ID); ok {
					offset["oldest"] = oldest
				}
				body := map[string]any{
					"@timestamp": cc.ts.UTC().Format(time.RFC3339Nano),
					"event":      map[string]any{"dataset": "kafka.partition", "module": "kafka"},
					"metricset":  map[string]any{"name": "partition", "period": period},
					"service":    map[string]any{"type": "kafka", "address": cc.brokers[p.Leader]},
					"kafka": map[string]any{
						"cluster": map[string]any{"name": c.cfg.Cluster, "id": cc.md.ClusterID},
						"broker":  map[string]any{"id": r, "address": cc.brokers[r]},
						"topic":   c.topicField(name, ""),
						"partition": map[string]any{
							"id":              p.ID,
							"topic_id":        fmt.Sprintf("%d-%s", p.ID, name),
							"topic_broker_id": fmt.Sprintf("%d-%s-%d", p.ID, name, r),
							"offset":          offset,
							"partition": map[string]any{
								"id":             p.ID,
								"leader":         p.Leader,
								"replica":        r,
								"insync_replica": contains(p.ISR, r),
								"is_leader":      r == p.Leader,
							},
						},
					},
				}
				c.enrich(body)
				docs = append(docs, sink.Doc{Stream: "metrics", Body: body})
			}
		}
	}
	return docs
}

// consumerGroupDoc produit, comme le metricset consumergroup de Metricbeat,
// un document par groupe, topic et partition commitée, complété par
// l'encadrement du lag de la partition ; émis même sans membre actif.
func (c *Collector) consumerGroupDoc(cc *cycleCtx, key pairKey, tm kafkaclient.TopicMeta, v partView) sink.Doc {
	coord := cc.groups[key.Group].Coordinator
	lag := map[string]any{
		"seconds_lower":     seconds(v.bracket.Lower),
		"method":            v.bracket.Method(),
		"last_commit_age_s": seconds(v.age),
		"stalled":           v.stalled,
	}
	if v.lagOff == 0 {
		lag["seconds_upper"] = 0.0
		lag["method"] = "bracket"
	} else if v.bracket.UpperKnown {
		lag["seconds_upper"] = seconds(v.bracket.Upper)
	} else {
		lag["seconds_upper"] = nil
	}
	if v.hasRetention {
		lag["retention_used"] = v.retention
		lag["commit_before_log_start"] = v.lost > 0
		if v.lost > 0 {
			lag["offsets_lost"] = v.lost
		}
	}
	group := map[string]any{
		"id":           key.Group,
		"offset":       v.commit.Offset,
		"consumer_lag": v.leo - v.commit.Offset,
		"meta":         v.commit.Metadata,
		"error":        map[string]any{"code": 0},
	}
	if m, ok := cc.members[key.Group][tm.Name][v.id]; ok {
		group["client"] = map[string]any{"id": m.ClientID, "host": m.ClientHost, "member_id": m.ID}
	}
	body := map[string]any{
		"@timestamp": cc.ts.UTC().Format(time.RFC3339Nano),
		"event":      map[string]any{"dataset": "kafka.consumergroup", "module": "kafka"},
		"metricset":  map[string]any{"name": "consumergroup", "period": c.cfg.Collect.MetricsInterval.Milliseconds()},
		"service":    map[string]any{"type": "kafka"},
		"kafka": map[string]any{
			"cluster":       map[string]any{"name": c.cfg.Cluster, "id": cc.md.ClusterID},
			"broker":        map[string]any{"id": coord, "address": cc.brokers[coord]},
			"topic":         c.topicField(tm.Name, ""),
			"partition":     map[string]any{"id": v.id, "topic_id": fmt.Sprintf("%d-%s", v.id, tm.Name)},
			"consumergroup": group,
			"lag":           lag,
		},
	}
	c.enrich(body)
	return sink.Doc{Stream: "metrics", Body: body}
}

// retention situe le commit dans le journal retenu [log start, LEO) : part
// des messages retenus que le groupe n'a pas encore lus, de 0 (à jour) à 1
// (commit au log start), et offsets perdus quand la rétention a dépassé le
// commit (part alors à 1). Inconnue tant que le log start vaut 0 (topic
// compacté ou rétention jamais atteinte). Le log start, lu aux cycles
// métriques, n'a que du retard : une perte vue est certaine.
func (c *Collector) retention(topic string, partition int32, leo, commit int64) (used float64, lost int64, ok bool) {
	start, found := c.logStart.Get(topic, partition)
	if !found || start <= 0 {
		return 0, 0, false
	}
	switch {
	case commit < start:
		return 1, start - commit, true
	case commit >= leo:
		return 0, 0, true
	}
	return float64(leo-commit) / float64(leo-start), 0, true
}

// groupField construit l'objet kafka.consumergroup d'un document de paire ;
// le type n'est connu qu'à partir de Kafka 3.8.
func groupField(cc *cycleCtx, g string) map[string]any {
	lg := cc.groups[g]
	out := map[string]any{"id": g, "state": lg.State}
	if lg.Type != "" {
		out["type"] = lg.Type
	}
	return out
}

func contains(xs []int32, x int32) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func (c *Collector) goneDoc(cc *cycleCtx, key pairKey, topicID string, partitions int) sink.Doc {
	st := c.pairs[key]
	body := map[string]any{
		"@timestamp": cc.ts.UTC().Format(time.RFC3339Nano),
		"event":      map[string]any{"dataset": "kafkabeat.lag", "module": "kafkabeat"},
		"kafka": map[string]any{
			"cluster":       map[string]any{"name": c.cfg.Cluster, "id": cc.md.ClusterID},
			"topic":         c.topicField(key.Topic, topicID),
			"consumergroup": groupField(cc, key.Group),
			"lag": map[string]any{
				"state":           StateGone,
				"partition_count": partitions,
				"flags":           c.flags(st, cc.h.incomplete),
			},
		},
	}
	c.enrich(body)
	return sink.Doc{Stream: "lag", Body: body}
}

func (c *Collector) flags(st *pairState, incomplete bool) map[string]bool {
	out := make(map[string]bool, len(flagNames))
	for _, f := range flagNames {
		out[f] = st.Flags[f] > 0
	}
	out[flagCycleIncompl] = incomplete
	return out
}

func (c *Collector) decayFlags(st *pairState) {
	for f, n := range st.Flags {
		if n <= 1 {
			delete(st.Flags, f)
		} else {
			st.Flags[f] = n - 1
		}
	}
}

func (c *Collector) healthDoc(cc *cycleCtx, start time.Time) sink.Doc {
	series, entries := c.hist.Stats()
	h := cc.h
	body := map[string]any{
		"@timestamp": cc.ts.UTC().Format(time.RFC3339Nano),
		"event":      map[string]any{"dataset": "kafkabeat.health", "module": "kafkabeat"},
		"kafka":      map[string]any{"cluster": map[string]any{"name": c.cfg.Cluster, "id": cc.md.ClusterID}},
		"kafkabeat": map[string]any{
			"version":           c.version,
			"cycle":             c.cycle,
			"cycle_duration_ms": time.Since(start).Milliseconds(),
			"cycle_incomplete":  h.incomplete,
			"cycle_failed":      h.failed,
			"errors":            h.errors,
			"partial_errors":    h.partialErrors,
			"group_errors":      h.groupErrors,
			"topics":            h.topics,
			"partitions":        h.partitions,
			"groups":            h.groups,
			"pairs":             h.pairs,
			"pairs_gone":        h.pairsGone,
			"pairs_unknown":     h.pairsUnknown,
			"docs":              h.docs,
			"rebalance_method":  cc.method,
			"commits_mode":      c.commits.Mode(),
			"history":           map[string]any{"series": series, "entries": entries},
			"last_snapshot":     timeOrNil(c.lastSnapshot),
		},
	}
	kb := body["kafkabeat"].(map[string]any)
	if s, ok := c.out.(interface{ Stats() map[string]any }); ok {
		kb["sink"] = s.Stats()
	}
	if s, ok := c.commits.(interface{ Stats() map[string]any }); ok {
		kb["stream"] = s.Stats()
	}
	if c.jolokia != nil {
		if section, errs := c.jolokia.Health(); section != nil {
			kb["jolokia"] = section
			kb["errors"] = append(h.errors, errs...)
		}
	}
	c.enrich(body)
	return sink.Doc{Stream: "health", Body: body}
}

func (c *Collector) enrich(body map[string]any) {
	for k, v := range c.cfg.Output.Fields {
		body[k] = v
	}
}

func timeOrNil(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func rate(delta int64, dt time.Duration) any {
	if dt <= 0 {
		return nil
	}
	return math.Round(float64(delta)/dt.Seconds()*1000) / 1000
}

func seconds(d time.Duration) float64 {
	return math.Round(d.Seconds()*1000) / 1000
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedPairKeys(m map[pairKey]*pairState) []pairKey {
	keys := make([]pairKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Group != keys[j].Group {
			return keys[i].Group < keys[j].Group
		}
		return keys[i].Topic < keys[j].Topic
	})
	return keys
}
