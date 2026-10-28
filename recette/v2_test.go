//go:build bench

package recette

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// harnessStatus est le GET /status d'un consumer du harnais.
type harnessStatus struct {
	MemberID   string     `json:"member_id"`
	Generation int32      `json:"generation"`
	Assigned   []string   `json:"assigned"`
	AssignedAt *time.Time `json:"assigned_at"`
}

func (c *consumer) status(t *testing.T) harnessStatus {
	t.Helper()
	var st harnessStatus
	if err := json.Unmarshal([]byte(c.must(t, http.MethodGet, "/status")), &st); err != nil {
		t.Fatal(err)
	}
	return st
}

// waitGeneration attend que le membre ait rejoint la génération gen au moins
// avec une assignation reçue.
func (b *bench) waitGeneration(c *consumer, gen int32) harnessStatus {
	b.t.Helper()
	var st harnessStatus
	b.waitFor(fmt.Sprintf("generation %d of %s", gen, c.control), 90*time.Second, func() bool {
		st = c.status(b.t)
		return st.Generation >= gen && st.AssignedAt != nil
	})
	return st
}

func eventDocs(t *testing.T, cluster, group string, since time.Time) []doc {
	return search(t, "kafkabeat-events", map[string]any{"kafka.cluster.name": cluster, "kafka.consumergroup.id": group}, since, 1000)
}

// memberIDs lit les identifiants d'une liste de membres (joined, left) d'un rebalance.
func memberIDs(d doc, list string) []string {
	rb, _ := d.src["kafka"].(map[string]any)["rebalance"].(map[string]any)
	raw, _ := rb[list].([]any)
	var out []string
	for _, m := range raw {
		out = append(out, m.(map[string]any)["member_id"].(string))
	}
	sort.Strings(out)
	return out
}

// Critère 10 : rebalances provoqués par le harnais (entrée, sortie, arrêt
// brutal) sur 3.9 et 4.4 : un document par génération, générations
// consécutives, membres entrés et sortis corrects, horodatage à un cycle
// près. Flux coupé (agent en mode polling, voir le plan) : méthode describe
// signalée, aucun rebalance sans changement de membres, un seul pour une entrée.
func TestC10Rebalances(t *testing.T) {
	for _, tc := range []struct{ name, kafka string }{{"kafka39", kafka39}, {"kafka44", kafka44}} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBench(t, tc.kafka)
			topic := b.topic("c10", 4)
			a := b.agent(agentOpts{})
			// balancer eager : une génération par changement de membres
			opts := []string{"--balancer", "range", "--session-timeout", "6s", "--commit", "auto", "--commit-interval", "1s"}
			first := b.consumer(b.name+"-g10", topic, opts...)
			group := first.group
			g0 := b.waitGeneration(first, 1).Generation
			since := time.Now()

			type want struct {
				joined, left string
				at           time.Time
			}
			var steps []want
			second := b.consumer(group, topic, opts...)
			st := b.waitGeneration(first, g0+1)
			steps = append(steps, want{joined: b.waitGeneration(second, g0+1).MemberID, at: *st.AssignedAt})
			third := b.consumer(group, topic, opts...)
			st = b.waitGeneration(first, g0+2)
			steps = append(steps, want{joined: b.waitGeneration(third, g0+2).MemberID, at: *st.AssignedAt})
			thirdID := third.status(t).MemberID
			third.stop()
			st = b.waitGeneration(first, g0+3)
			steps = append(steps, want{left: thirdID, at: *st.AssignedAt})
			secondID := second.status(t).MemberID
			second.cmd.Process.Kill() // arrêt brutal : départ vu à l'expiration de la session (6 s)
			st = b.waitGeneration(first, g0+4)
			steps = append(steps, want{left: secondID, at: *st.AssignedAt})

			var ev []doc
			b.waitFor("4 rebalances", 30*time.Second, func() bool {
				ev = eventDocs(t, a.cluster, group, since)
				return len(ev) >= 4
			})
			time.Sleep(3 * interval)
			if ev = eventDocs(t, a.cluster, group, since); len(ev) != 4 {
				t.Fatalf("expected 4 rebalances, %d documents", len(ev))
			}
			for i, d := range ev {
				w := steps[i]
				gen := float64(g0) + float64(i) + 1
				if d.str("kafka.rebalance.method") != "stream" || d.num("kafka.consumergroup.generation") != gen ||
					d.num("kafka.rebalance.previous_generation") != gen-1 {
					t.Fatalf("rebalance %d: expected generation %v (previous %v) from the stream: %v", i+1, gen, gen-1, d.src)
				}
				joined, left := memberIDs(d, "joined"), memberIDs(d, "left")
				if (w.joined != "") != (len(joined) == 1) || (w.joined != "" && joined[0] != w.joined) ||
					(w.left != "") != (len(left) == 1) || (w.left != "" && left[0] != w.left) {
					t.Fatalf("rebalance %d: expected joined %q left %q, got %v and %v", i+1, w.joined, w.left, joined, left)
				}
				if dt := d.ts.Sub(w.at); dt > interval || dt < -interval {
					t.Fatalf("rebalance %d: timestamp %s, assignment seen at %s (gap %s)", i+1, d.ts.Format(time.TimeOnly), w.at.Format(time.TimeOnly), dt)
				}
			}
			if h := lastDoc(healthDocs(t, a.cluster, since)); h.str("kafkabeat.rebalance_method") != "stream" {
				t.Fatalf("expected method stream in health: %v", h.f("kafkabeat.rebalance_method"))
			}

			// flux coupé : le groupe existe avant l'agent (référence), rien ne
			// change pendant deux cycles métriques, puis un membre entre
			p1 := b.consumer(b.name+"-g10p", topic, opts...)
			gp := b.waitGeneration(p1, 1).Generation
			ap := b.agent(agentOpts{commits: "polling"})
			psince := time.Now()
			time.Sleep(45 * time.Second)
			if ev := eventDocs(t, ap.cluster, p1.group, psince); len(ev) != 0 {
				t.Fatalf("no rebalance expected without a member change: %d document(s)", len(ev))
			}
			if h := lastDoc(healthDocs(t, ap.cluster, psince)); h.str("kafkabeat.rebalance_method") != "describe" {
				t.Fatalf("expected describe fallback in health: %v", h.f("kafkabeat.rebalance_method"))
			}
			p2 := b.consumer(p1.group, topic, opts...)
			joinedID := b.waitGeneration(p2, gp+1).MemberID
			b.waitFor("rebalance seen by DescribeGroups", 60*time.Second, func() bool {
				return len(eventDocs(t, ap.cluster, p1.group, psince)) > 0
			})
			time.Sleep(25 * time.Second) // un cycle métriques de plus : pas de doublon
			ev = eventDocs(t, ap.cluster, p1.group, psince)
			if len(ev) != 1 || ev[0].str("kafka.rebalance.method") != "describe" || ev[0].f("kafka.consumergroup.generation") != nil ||
				len(memberIDs(ev[0], "joined")) != 1 || memberIDs(ev[0], "joined")[0] != joinedID {
				t.Fatalf("expected a describe rebalance (%s joining): %v", joinedID, ev)
			}
			t.Logf("%s: generations %d to %d, describe fallback verified", tc.name, g0+1, g0+4)
		})
	}
}

// proxy relaie un port TCP ; en mode muet il accepte les connexions sans
// jamais répondre (Jolokia coupé du point de vue de l'agent).
type proxy struct {
	ln     net.Listener
	port   int
	target string
	mu     sync.Mutex
	mute   bool
	conns  []net.Conn
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, port: ln.Addr().(*net.TCPAddr).Port, target: target}
	t.Cleanup(func() { ln.Close(); p.setMute(true) })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			p.conns = append(p.conns, c)
			mute := p.mute
			p.mu.Unlock()
			if mute {
				continue
			}
			go func() {
				up, err := net.Dial("tcp", p.target)
				if err != nil {
					c.Close()
					return
				}
				go io.Copy(up, c)
				io.Copy(c, up)
				c.Close()
				up.Close()
			}()
		}
	}()
	return p
}

// setMute passe en mode muet (connexions en cours fermées) ou relais.
func (p *proxy) setMute(m bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mute = m
	if m {
		for _, c := range p.conns {
			c.Close()
		}
		p.conns = nil
	}
}

// brokerKey identifie un document broker : MBean à joker, ou "" pour le document des MBeans fixes.
func brokerKey(d doc) string { return d.str("kafka.broker.mbean") }

// brokerValues renvoie les champs numériques kafka.broker.* d'un document
// (hors id), ceux que produit le metricset broker.
func brokerValues(flat map[string]any) map[string]float64 {
	out := make(map[string]float64)
	for k, v := range flat {
		if n, ok := v.(float64); ok && strings.HasPrefix(k, "kafka.broker.") && k != "kafka.broker.id" {
			out[k] = n
		}
	}
	return out
}

// metricbeatFixed relit, dans le journal des événements rejetés de Metricbeat,
// ses documents des MBeans fixes : avec Jolokia 2.x (kafka44), un MBean absent
// en KRaft y figure en objet d'erreur et Elasticsearch rejette tout le
// document ; avec Jolokia 1.x (kafka39), ils sont dans l'index.
func metricbeatFixed(t *testing.T, cluster string) []doc {
	t.Helper()
	out, err := exec.Command("docker", "exec", "metricbeat", "sh", "-c", "tail -n 3000 /usr/share/metricbeat/logs/metricbeat-events-data-*.ndjson").Output()
	if err != nil {
		t.Fatalf("Metricbeat event log: %v", err)
	}
	var docs []doc
	for _, line := range bytes.Split(out, []byte("\n")) {
		var entry struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(line, &entry) != nil {
			continue
		}
		_, ev, ok := strings.Cut(entry.Message, "Cannot index event '")
		if !ok {
			continue
		}
		var src map[string]any
		if json.NewDecoder(strings.NewReader(ev)).Decode(&src) != nil {
			continue
		}
		flat := make(map[string]any)
		flatten("", src, flat)
		if flat["event.dataset"] != "kafka.broker" || flat["bench_cluster"] != cluster || flat["kafka.broker.mbean"] != nil {
			continue
		}
		ts, _ := time.Parse(time.RFC3339Nano, flat["@timestamp"].(string))
		docs = append(docs, doc{ts: ts, src: src, flat: flat})
	}
	return docs
}

// brokerMapping lit le type de chaque champ kafka.broker.* dans l'index le plus récent.
func brokerMapping(t *testing.T, index string) map[string]string {
	t.Helper()
	resp, err := http.Get(esURL + "/" + index + "/_mapping/field/kafka.broker.*")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var m map[string]struct {
		Mappings map[string]struct {
			Mapping map[string]struct {
				Type string `json:"type"`
			} `json:"mapping"`
		} `json:"mappings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	var latest string
	for name := range m {
		if name > latest {
			latest = name
		}
	}
	out := make(map[string]string)
	for field, f := range m[latest].Mappings {
		for _, v := range f.Mapping {
			out[field] = v.Type
		}
	}
	return out
}

// Critère 11 : metricset broker par Jolokia sur les brokers du banc (1.x sur
// 3.9, 2.x sur 4.4) :
// kafka.broker.* égaux à Metricbeat à une période près (chaque valeur de
// kafkabeat entre les valeurs de Metricbeat lues dans la période qui
// l'encadre, à partir du premier relevé Metricbeat de chaque MBean),
// mapping identique champ à champ ; Jolokia coupé (proxy muet) :
// erreur dans la santé, cycle de 10 s tenu. « Autres brokers émis » : tests
// unitaires de internal/jolokia (un broker par cluster sur le banc).
func TestC11BrokerMetricset(t *testing.T) {
	for _, tc := range []struct {
		name, kafka string
		port        int
	}{{"kafka39", kafka39, 8778}, {"kafka44", kafka44, 8779}} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBench(t, tc.kafka)
			topic := b.topic("c11", 2)
			prod, _ := b.producer(topic, "steady:100:10s", true)
			defer stopCmd(prod)
			cons := b.consumer(b.name+"-g11", topic, "--commit", "auto")
			px := newProxy(t, fmt.Sprintf("localhost:%d", tc.port))
			a := b.agent(agentOpts{jolokiaPort: px.port})
			startAt := time.Now()
			period := 20 * time.Second // metrics_interval de la recette

			var kb []doc
			b.waitFor("three Jolokia polls", 150*time.Second, func() bool {
				kb = search(t, "kafkabeat-metrics", map[string]any{"kafka.cluster.name": a.cluster, "event.dataset": "kafka.broker"}, startAt, 5000)
				fixed := 0
				for _, d := range kb {
					if brokerKey(d) == "" {
						fixed++
					}
				}
				return fixed >= 3
			})
			time.Sleep(period + 10*time.Second) // Metricbeat encadre la dernière valeur
			mb := search(t, "metricbeat-*", map[string]any{"bench_cluster": tc.name, "event.dataset": "kafka.broker"}, startAt.Add(-period), 10000)
			mb = append(mb, metricbeatFixed(t, tc.name)...)
			// Un MBean de topic naît au premier message : relu par kafkabeat avant le premier calcul de son débit, il
			// vaut 0, alors que Metricbeat (déphasé) peut ne le lire qu'après ; les relevés antérieurs au premier
			// relevé Metricbeat du MBean n'ont pas de référence et ne sont pas comparés.
			firstMB := map[string]time.Time{}
			for _, m := range mb {
				if k := brokerKey(m); firstMB[k].IsZero() || m.ts.Before(firstMB[k]) {
					firstMB[k] = m.ts
				}
			}

			checked := 0
			for _, d := range kb {
				key := brokerKey(d)
				if key != "" && !strings.Contains(key, "topic="+topic+",") {
					continue // MBeans des topics d'autres tests
				}
				if first, ok := firstMB[key]; ok && d.ts.Before(first) {
					continue
				}
				// l'horodatage de kafkabeat est aligné sur la grille de 10 s : lecture dans les 10 s qui suivent
				from, to := d.ts.Add(-period), d.ts.Add(10*time.Second+period)
				ref := map[string][]float64{}
				for _, m := range mb {
					if brokerKey(m) != key || m.ts.Before(from) || m.ts.After(to) {
						continue
					}
					for f, v := range brokerValues(m.flat) {
						ref[f] = append(ref[f], v)
					}
				}
				mine := brokerValues(d.flat)
				for f := range ref {
					if _, ok := mine[f]; !ok {
						t.Fatalf("%q: field %s published by Metricbeat, missing from kafkabeat", key, f)
					}
				}
				for f, v := range mine {
					vals := ref[f]
					if len(vals) == 0 {
						t.Fatalf("%q at %s: field %s has no Metricbeat value in the period", key, d.ts.Format(time.TimeOnly), f)
					}
					lo, hi := vals[0], vals[0]
					for _, x := range vals {
						lo, hi = math.Min(lo, x), math.Max(hi, x)
					}
					tol := math.Max(1e-6, 0.02*math.Max(math.Abs(lo), math.Abs(hi)))
					if v < lo-tol || v > hi+tol {
						t.Fatalf("%q at %s: %s = %v outside Metricbeat values [%v, %v]", key, d.ts.Format(time.TimeOnly), f, v, lo, hi)
					}
					checked++
				}
			}
			if checked < 10 {
				t.Fatalf("too few values compared: %d", checked)
			}
			theirs, mine := brokerMapping(t, "metricbeat-*"), brokerMapping(t, "kafkabeat-metrics")
			for f, typ := range theirs {
				if mine[f] != typ {
					t.Fatalf("mapping of %s: %q in kafkabeat, %q in Metricbeat", f, mine[f], typ)
				}
			}

			// Jolokia coupé : l'agent attend sa réponse jusqu'au délai (5 s)
			px.setMute(true)
			cutAt := time.Now()
			b.waitFor("Jolokia error in health", 60*time.Second, func() bool {
				h := healthDocs(t, a.cluster, cutAt)
				return len(h) > 0 && lastDoc(h).num("kafkabeat.jolokia.brokers_failed") == 1 &&
					strings.Contains(fmt.Sprint(lastDoc(h).f("kafkabeat.errors")), "jolokia: broker")
			})
			time.Sleep(period)
			h := healthDocs(t, a.cluster, cutAt)
			if g := gaps(h, 2*interval); len(g) > 0 {
				t.Fatalf("cycle not kept during the Jolokia outage: %v", head(g, 3))
			}
			for _, d := range h {
				if d.num("kafkabeat.cycle_duration_ms") >= float64(interval.Milliseconds()) {
					t.Fatalf("%v ms cycle during the Jolokia outage", d.f("kafkabeat.cycle_duration_ms"))
				}
			}
			if n := len(lagDocs(t, a.cluster, topic, cons.group, cutAt)); n < len(h)-2 {
				t.Fatalf("lag documents interrupted during the outage: %d for %d cycles", n, len(h))
			}
			t.Logf("%s: %d values equal to Metricbeat, %d identical mapping fields", tc.name, checked, len(theirs))
		})
	}
}

// Critère 12 : groupe à deux membres sur un topic de 4 partitions, dont
// l'assignateur du harnais omet une partition : partitions_unassigned = 1 et
// paire KO (état unassigned) au-delà de stalled_after (10 s sur la recette),
// pas avant ni pendant un rebalance ; groupe abonné sans commit : paire
// no_commit ; consumer assign() qui commite : classé manual, aucune alerte.
func TestC12Coverage(t *testing.T) {
	b := newBench(t, kafka39)
	topic := b.topic("c12", 4)
	prod, _ := b.producer(topic, "steady:20:10s", true)
	defer stopCmd(prod)
	a := b.agent(agentOpts{})
	omit := []string{"--balancer", "omit=" + topic + "/3", "--commit", "auto", "--commit-interval", "1s"}
	// le premier membre retient chaque rebalance 25 s (groupe en PreparingRebalance)
	m1 := b.consumer(b.name+"-g12", topic, append(omit, "--revoke-delay", "25s")...)
	group := m1.group
	b.waitGeneration(m1, 1)
	m2 := b.consumer(group, topic, omit...)
	g := b.waitGeneration(m2, 2).Generation
	b.waitGeneration(m1, g)
	stableAt := time.Now()

	unassigned := func(d doc) bool {
		ids, _ := d.f("kafka.lag.partitions_unassigned_ids").([]any)
		return d.num("kafka.lag.partitions_unassigned") == 1 && len(ids) == 1 && ids[0] == 3.0
	}
	var docs []doc
	b.waitFor("pair not OK", 60*time.Second, func() bool {
		docs = lagDocs(t, a.cluster, topic, group, stableAt)
		return len(docs) > 0 && lastDoc(docs).str("kafka.lag.state") == "unassigned"
	})
	sawBefore := false
	for _, d := range docs {
		if d.str("kafka.consumergroup.state") != "Stable" {
			continue // état lu par ListGroups un cycle après la fin du rebalance
		}
		if !unassigned(d) {
			t.Fatalf("expected only partition 3 unassigned: %v", d.src["kafka"])
		}
		age := d.num("kafka.lag.unassigned_age_max_s")
		// âge publié arrondi à la milliseconde : le seuil exact n'est pas départagé
		if ko := d.str("kafka.lag.state") == "unassigned"; ko != (age >= 10) && math.Abs(age-10) > 0.01 {
			t.Fatalf("not OK expected only beyond 10 s: age %v, state %s", age, d.str("kafka.lag.state"))
		}
		sawBefore = sawBefore || age < 10
	}
	if !sawBefore {
		t.Fatal("no document before the threshold: unassignment seen too late")
	}

	// rebalance tenu 25 s par le premier membre : ni couverture ni KO pendant
	m3 := b.consumer(group, topic, omit...)
	rebalanceAt := time.Now()
	g = b.waitGeneration(m3, g+1).Generation
	b.waitGeneration(m1, g)
	during := 0
	for _, d := range lagDocs(t, a.cluster, topic, group, rebalanceAt) {
		if s := d.str("kafka.consumergroup.state"); s != "PreparingRebalance" && s != "CompletingRebalance" {
			continue
		}
		during++
		if d.str("kafka.lag.state") == "unassigned" || d.f("kafka.lag.partitions_unassigned") != nil {
			t.Fatalf("no unassigned partition expected during a rebalance: %v", d.src["kafka"])
		}
	}
	if during < 5 {
		t.Fatalf("rebalance too short to be observed: %d document(s)", during)
	}
	afterAt := time.Now()
	b.waitFor("not OK again after the rebalance", 60*time.Second, func() bool {
		d := lagDocs(t, a.cluster, topic, group, afterAt)
		return len(d) > 0 && lastDoc(d).str("kafka.lag.state") == "unassigned" && unassigned(lastDoc(d))
	})

	// groupe abonné qui ne commite jamais, consumer assign() qui commite
	sub := b.consumer(b.name+"-g12s", topic, "--commit", "manual")
	manual := b.consumer(b.name+"-g12m", topic, "--assign", "--commit", "auto", "--commit-interval", "1s")
	otherAt := time.Now()
	b.waitFor("pair without commit", 90*time.Second, func() bool {
		d := lagDocs(t, a.cluster, topic, sub.group, otherAt)
		return len(d) > 0 && lastDoc(d).str("kafka.lag.state") == "no_commit"
	})
	b.waitFor("group with manual assignment", 90*time.Second, func() bool {
		d := lagDocs(t, a.cluster, topic, manual.group, otherAt)
		return len(d) > 2 && lastDoc(d).str("kafka.consumergroup.assignment") == "manual"
	})
	time.Sleep(15 * time.Second) // au-delà de stalled_after : toujours sans alerte
	for _, d := range lagDocs(t, a.cluster, topic, manual.group, otherAt.Add(10*time.Second)) {
		if s := d.str("kafka.lag.state"); s == "unassigned" || s == "stalled" || d.f("kafka.lag.partitions_unassigned") != nil ||
			d.num("kafka.consumergroup.members") != 0 {
			t.Fatalf("committing assign(): no alert expected: %v", d.src["kafka"])
		}
	}
	// tuiles v2 de la vue d'ensemble (requêtes du ndjson livré, filtre cluster) ;
	// elles retiennent le pire état des 30 dernières secondes : lues une fois
	// la fenêtre passée sur l'état stable
	nowWindow := 35 * time.Second
	time.Sleep(nowWindow)
	cluster := map[string]any{"kafka.cluster.name": a.cluster}
	checkTiles(t, "partition without member", tiles(t, cluster), map[string]float64{"Unassigned partitions": 1, "Groups without members": 0})
	// membres arrêtés : groupe sans membre aux commits figés ; assign() reste à part
	for _, c := range []*consumer{m3, m2, m1} {
		c.stop()
	}
	emptyAt := time.Now()
	b.waitFor("group without members", 60*time.Second, func() bool {
		d := lagDocs(t, a.cluster, topic, group, emptyAt)
		return len(d) > 0 && lastDoc(d).str("kafka.consumergroup.assignment") == "none"
	})
	time.Sleep(nowWindow)
	checkTiles(t, "group without members", tiles(t, cluster), map[string]float64{"Unassigned partitions": 0, "Groups without members": 1})
	t.Logf("not OK after 10 s; %d documents during the rebalance, none not OK", during)
}
