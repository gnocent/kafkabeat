//go:build bench

package recette

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/gnocent/kafkabeat/bench/harness/journal"
)

// Critère 1 : le vrai lag du dernier offset commité est entre les bornes sur
// 100 % des cycles ; largeur <= un intervalle dans la fenêtre fine.
func TestC1BoundsContainTruth(t *testing.T) {
	b := newBench(t, kafka39)
	topic := b.topic("c1", 6)
	cons := b.consumer(b.name+"-g1", topic, "--commit", "auto", "--commit-interval", "1s", "--max-rate", "150")
	prod, journalPath := b.producer(topic, "steady:300:20s,silence:0:10s,burst:3000:4s,steady:80:15s", true)
	a := b.agent(agentOpts{})
	duration := 3 * time.Minute
	if os.Getenv("KB_LONG") != "" {
		duration = 2 * time.Hour
	}
	start := time.Now()
	time.Sleep(duration)
	stopCmd(prod)
	time.Sleep(2 * interval)

	jr, err := journal.Open(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	docs := lagDocs(t, a.cluster, topic, cons.group, start)
	var checks int
	var violations, widths []string
	tol := interval.Seconds() + 0.5
	for _, d := range docs {
		for _, p := range d.partitions() {
			off, ok := p["offset_committed"].(float64)
			if !ok {
				continue
			}
			upper, hasUpper := p["seconds_upper"].(float64)
			if !hasUpper {
				continue
			}
			lower := p["seconds_lower"].(float64)
			producedAt, ok := jr.ProducedAt(topic, int32(p["id"].(float64)), int64(off))
			if !ok {
				continue
			}
			// âge à l'instant du cycle (grille) ; l'instant réel est dans [ts, ts+interval)
			truth := d.ts.Sub(producedAt).Seconds()
			checks++
			if lower > truth+tol || upper+0.5 < truth {
				violations = append(violations, fmt.Sprintf("%s p%v offset %v: truth %.1fs outside [%.1f, %.1f]", d.ts.Format(time.TimeOnly), p["id"], off, truth, lower, upper))
			}
			if truth < 14*60 && upper-lower > tol {
				widths = append(widths, fmt.Sprintf("%s p%v: width %.1fs", d.ts.Format(time.TimeOnly), p["id"], upper-lower))
			}
		}
	}
	t.Logf("criterion 1: %d documents, %d brackets checked, %d violations, %d excessive widths", len(docs), checks, len(violations), len(widths))
	if checks < 50 {
		t.Fatalf("too few checkable brackets: %d", checks)
	}
	if len(violations) > 0 {
		t.Fatalf("truth outside bounds:\n%s", strings.Join(head(violations, 20), "\n"))
	}
	if len(widths) > 0 {
		t.Fatalf("width > interval in the fine window:\n%s", strings.Join(head(widths, 20), "\n"))
	}
}

// Critère 2 : consumer bloqué puis production arrêtée : la borne haute croît
// d'un intervalle par cycle, un document par cycle, aucun trou ; état stalled.
func TestC2StalledKeepsEmitting(t *testing.T) {
	b := newBench(t, kafka39)
	topic := b.topic("c2a", 3)
	cons := b.consumer(b.name+"-g2a", topic, "--commit", "auto", "--commit-interval", "1s")
	prod, _ := b.producer(topic, "steady:100:15s", false)
	a := b.agent(agentOpts{})
	time.Sleep(6 * time.Second)
	cons.must(t, "POST", "/pause")
	pausedAt := time.Now()
	prod.Wait() // la production s'arrête ~9 s plus tard
	time.Sleep(40 * time.Second)

	docs := lagDocs(t, a.cluster, topic, cons.group, pausedAt.Add(3*interval))
	if len(docs) < 15 {
		t.Fatalf("expected at least 15 documents after the stall, got %d", len(docs))
	}
	if g := gaps(docs, interval+600*time.Millisecond); len(g) > 0 {
		t.Fatalf("gaps in the series: %v", g)
	}
	var deltas []string
	for i := 1; i < len(docs); i++ {
		d := docs[i].num("kafka.lag.seconds_upper") - docs[i-1].num("kafka.lag.seconds_upper")
		if math.Abs(d-interval.Seconds()) > 0.6 {
			deltas = append(deltas, fmt.Sprintf("%s: +%.2fs", docs[i].ts.Format(time.TimeOnly), d))
		}
		if docs[i].num("kafka.lag.offsets_sum") <= 0 {
			t.Fatalf("offset lag must stay positive: %v", docs[i].flat["kafka.lag.offsets_sum"])
		}
	}
	if len(deltas) > 0 {
		t.Fatalf("upper bound must grow by one interval per cycle: %v", deltas)
	}
	if s := lastDoc(docs).str("kafka.lag.state"); s != "stalled" {
		t.Fatalf("expected state stalled, got %s", s)
	}
}

// Critère 2 bis : silence puis rafale consommée aussitôt : la borne haute
// n'englobe pas le silence.
func TestC2SilenceThenBurst(t *testing.T) {
	b := newBench(t, kafka39)
	topic := b.topic("c2b", 3)
	cons := b.consumer(b.name+"-g2b", topic, "--commit", "auto", "--commit-interval", "1s", "--max-rate", "100")
	a := b.agent(agentOpts{})
	const silence = 60 * time.Second
	prod, _ := b.producer(topic, "steady:30:10s,silence:0:60s,burst:3000:3s", false)
	burstAt := time.Now().Add(10*time.Second + silence)
	prod.Wait()
	time.Sleep(12 * time.Second)

	docs := lagDocs(t, a.cluster, topic, cons.group, burstAt)
	var lagging int
	for _, d := range docs {
		if d.num("kafka.lag.offsets_sum") <= 0 {
			continue
		}
		lagging++
		upper := d.num("kafka.lag.seconds_upper")
		limit := d.ts.Sub(burstAt).Seconds() + 2*interval.Seconds() + 1
		if upper > limit || upper >= silence.Seconds()/2 {
			t.Fatalf("%s: upper bound %.1fs spans the silence (limit %.1fs)", d.ts.Format(time.TimeOnly), upper, limit)
		}
	}
	if lagging == 0 {
		t.Fatal("no lagging document after the burst: scenario not representative")
	}
	t.Logf("criterion 2b: %d lagging documents after the burst, upper bounds bounded", lagging)
}

// Critère 3 : reset d'offset, topic recréé, groupe supprimé, Kafka injoignable.
func TestC3FlagsAndGone(t *testing.T) {
	b := newBench(t, kafka39)
	a := b.agent(agentOpts{})

	// 3a : reset vers le début -> offset_reset_suspected pendant flag_hold_cycles
	t1 := b.topic("c3a", 3)
	c1 := b.consumer(b.name+"-g3a", t1, "--commit", "auto", "--commit-interval", "1s", "--max-rate", "50")
	p1, _ := b.producer(t1, "steady:200:10s", false)
	p1.Wait()
	b.waitDocs(a.cluster, t1, c1.group, time.Time{}, 3)
	c1.must(t, "POST", "/reset?to=earliest")
	resetAt := time.Now()
	var raised []doc
	b.waitFor("flag offset_reset_suspected", 60*time.Second, func() bool {
		for _, d := range lagDocs(t, a.cluster, t1, c1.group, resetAt) {
			if d.boolean("kafka.lag.flags.offset_reset_suspected") {
				raised = append(raised, d)
				return true
			}
		}
		return false
	})
	time.Sleep(8 * interval)
	var held, dropped int
	for _, d := range lagDocs(t, a.cluster, t1, c1.group, raised[0].ts) {
		if d.boolean("kafka.lag.flags.offset_reset_suspected") {
			held++
		} else {
			dropped++
		}
	}
	if held < 3 || dropped == 0 {
		t.Fatalf("flag held %d cycles (3 expected) then cleared on %d documents", held, dropped)
	}

	// 3b : topic recréé -> topic_recreated
	t2 := b.topic("c3b", 2)
	c2 := b.consumer(b.name+"-g3b", t2, "--commit", "auto", "--commit-interval", "1s")
	p2, _ := b.producer(t2, "steady:100:5s", false)
	p2.Wait()
	b.waitDocs(a.cluster, t2, c2.group, time.Time{}, 3)
	b.harness("topics", "recreate", "--name", t2, "--partitions", "2")
	recreatedAt := time.Now()
	b.waitFor("flag topic_recreated", 60*time.Second, func() bool {
		for _, d := range lagDocs(t, a.cluster, t2, c2.group, recreatedAt.Add(-interval)) {
			if d.boolean("kafka.lag.flags.topic_recreated") {
				return true
			}
		}
		return false
	})

	// 3c : groupe supprimé -> gone pendant gone_cycles puis purge
	t3 := b.topic("c3c", 2)
	c3 := b.consumer(b.name+"-g3c", t3, "--commit", "auto", "--commit-interval", "1s")
	p3, _ := b.producer(t3, "steady:100:5s", false)
	p3.Wait()
	b.waitDocs(a.cluster, t3, c3.group, time.Time{}, 3)
	c3.stop()
	b.harness("groups", "delete", "--group", c3.group)
	deletedAt := time.Now()
	b.waitFor("documents gone", 60*time.Second, func() bool {
		return countState(lagDocs(t, a.cluster, t3, c3.group, deletedAt), "gone") >= 3
	})
	time.Sleep(6 * interval)
	after := lagDocs(t, a.cluster, t3, c3.group, deletedAt)
	if n := countState(after, "gone"); n != 3 {
		t.Fatalf("expected 3 gone documents before purge, got %d", n)
	}
	if last := lastDoc(after); last.str("kafka.lag.state") != "gone" {
		t.Fatalf("no document may follow the purge: %v", last.flat["kafka.lag.state"])
	}

	// 3d : Kafka injoignable (pause de 45 s, au-delà des délais du client) ->
	// santé en erreur, état conservé, pas de gone, reprise ensuite
	docker(t, "pause", "kafka39")
	pausedAt := time.Now()
	unpaused := false
	defer func() {
		if !unpaused {
			docker(t, "unpause", "kafka39")
		}
	}()
	time.Sleep(45 * time.Second)
	docker(t, "unpause", "kafka39")
	unpaused = true
	resumedAt := time.Now()
	b.waitFor("health document with error", 60*time.Second, func() bool {
		for _, h := range healthDocs(t, a.cluster, pausedAt) {
			if h.boolean("kafkabeat.cycle_incomplete") && h.f("kafkabeat.errors") != nil {
				return true
			}
		}
		return false
	})
	b.waitFor("lag documents resuming", 120*time.Second, func() bool {
		docs := lagDocs(t, a.cluster, t1, c1.group, resumedAt)
		return len(docs) >= 2
	})
	for _, d := range lagDocs(t, a.cluster, t1, c1.group, pausedAt) {
		if d.str("kafka.lag.state") == "gone" {
			t.Fatalf("a collection failure must not produce gone: %s", d.ts)
		}
	}
}

// Critère 4 : redémarrage après un arrêt long : bornes reprises depuis le
// disque, resumed_after_gap, aucun taux sur l'intervalle.
func TestC4RestartAfterGap(t *testing.T) {
	b := newBench(t, kafka39)
	topic := b.topic("c4", 3)
	cons := b.consumer(b.name+"-g4", topic, "--commit", "auto", "--commit-interval", "1s", "--max-rate", "50")
	prod, _ := b.producer(topic, "steady:200:10s", false)
	a := b.agent(agentOpts{snapshot: 5 * time.Second})
	prod.Wait()
	time.Sleep(5 * time.Second)
	cons.must(t, "POST", "/pause")
	time.Sleep(4 * interval)
	before := lastDoc(b.waitDocs(a.cluster, topic, cons.group, time.Time{}, 3))
	a.stop()
	stoppedAt := time.Now()
	for _, f := range []string{"state.gob", "history.snap"} {
		if _, err := os.Stat(a.stateDir + "/" + f); err != nil {
			t.Fatalf("final snapshot missing: %v", err)
		}
	}
	const gap = 15 * time.Second
	time.Sleep(gap)
	a.start()
	docs := b.waitDocs(a.cluster, topic, cons.group, stoppedAt, 2)
	first := docs[0]
	if !first.boolean("kafka.lag.flags.resumed_after_gap") {
		t.Fatalf("expected resumed_after_gap: %v", first.flat["kafka.lag.flags.resumed_after_gap"])
	}
	if first.f("kafka.lag.produce_rate") != nil || first.f("kafka.lag.interval_ms") != nil {
		t.Fatalf("expected no rate over the downtime interval: %v %v", first.f("kafka.lag.produce_rate"), first.f("kafka.lag.interval_ms"))
	}
	if !strings.Contains(a.log(), "state restored") {
		t.Fatalf("the agent must announce the restored state:\n%s", a.log())
	}
	// offsets figés : l'âge croît exactement du temps écoulé, à un cycle près
	elapsed := first.ts.Sub(before.ts).Seconds()
	tol := interval.Seconds() + 1
	for _, k := range []string{"kafka.lag.seconds_lower", "kafka.lag.seconds_upper"} {
		got := first.num(k) - before.num(k)
		if math.Abs(got-elapsed) > tol {
			t.Fatalf("%s: +%.1fs for %.1fs elapsed (bounds not restored from disk)", k, got, elapsed)
		}
	}
	if docs[1].f("kafka.lag.produce_rate") == nil {
		t.Fatal("rates must resume from the second cycle")
	}
}

// Critère 5 : flux __consumer_offsets sur 3.9 et 4.4 ; injection d'une valeur
// de commit en version inconnue : groupe basculé sur polling, compteur
// incrémenté, commits toujours suivis et égaux à OffsetFetch une fois figés.
//
// L'injection abîme durablement la partition de __consumer_offsets qui la
// reçoit : l'ancien coordinateur (3.9) ne la relit qu'au redémarrage, le
// nouveau (4.x) la recharge aussitôt, échoue (UnknownRecordVersionException)
// et retire les groupes de cette partition. Les injections sont donc
// confinées à la partition de quarantaine : sur 3.9 le groupe suivi y est
// placé (repli exercé de bout en bout) ; sur 4.4 un groupe jetable y est
// placé et le groupe suivi, ailleurs, doit continuer à être servi par le
// flux. Après la recette, remettre le banc à neuf (make bench-down).
func TestC5StreamFallback(t *testing.T) {
	for _, tc := range []struct {
		name, kafka   string
		injectTracked bool
	}{{"kafka39", kafka39, true}, {"kafka44", kafka44, false}} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBench(t, tc.kafka)
			topic := b.topic("c5", 3)
			group := groupOff(b.name+"-g5", quarantinePartition)
			if tc.injectTracked {
				group = groupOn(b.name+"-g5", quarantinePartition)
			}
			cons := b.consumerRaw(group, topic, "--commit", "auto", "--commit-interval", "1s", "--max-rate", "100")
			prod, _ := b.producer(topic, "steady:300:5s,silence:0:5s", true)
			defer stopCmd(prod)
			a := b.agent(agentOpts{reconcile: 10 * time.Minute})
			startAt := time.Now()
			b.waitFor("stream records", 60*time.Second, func() bool {
				h := healthDocs(t, a.cluster, startAt)
				return len(h) > 0 && lastDoc(h).num("kafkabeat.stream.records") > 0 && lastDoc(h).boolean("kafkabeat.stream.ready")
			})
			d := lastDoc(b.waitDocs(a.cluster, topic, cons.group, startAt, 3))
			if age := d.num("kafka.lag.last_commit_age_max_s"); age > 5 {
				t.Fatalf("commit age must come from the stream (commit every 1 s), got %.1fs", age)
			}

			injected := cons.group
			if !tc.injectTracked {
				injected = groupOn(b.name+"-jetable", quarantinePartition)
			}
			injectUnknownValue(t, tc.kafka, injected, topic)
			injectedAt := time.Now()
			b.waitFor("group fallback", 60*time.Second, func() bool {
				h := healthDocs(t, a.cluster, injectedAt)
				return len(h) > 0 && lastDoc(h).num("kafkabeat.stream.unknown_values") >= 1 && lastDoc(h).num("kafkabeat.stream.groups_fallback") >= 1
			})
			d1 := lastDoc(b.waitDocs(a.cluster, topic, cons.group, injectedAt, 1))
			time.Sleep(10 * time.Second)
			d2 := lastDoc(lagDocs(t, a.cluster, topic, cons.group, injectedAt))
			if d2.num("kafka.lag.consumed_total") <= d1.num("kafka.lag.consumed_total") {
				t.Fatalf("after the fallback, commits must keep advancing: %v then %v", d1.f("kafka.lag.consumed_total"), d2.f("kafka.lag.consumed_total"))
			}
			if s := d2.str("kafka.lag.state"); s == "gone" || s == "no_commit" {
				t.Fatalf("unexpected state after fallback: %s", s)
			}
			h := lastDoc(healthDocs(t, a.cluster, injectedAt))
			if h.boolean("kafkabeat.cycle_incomplete") {
				t.Fatalf("the cycle must not be incomplete after the injection: %v", h.f("kafkabeat.errors"))
			}
			// valeurs égales au polling à un cycle près : commits figés, la vue
			// de l'agent (repli sur 3.9, flux sur 4.4) doit égaler OffsetFetch
			cons.must(t, "POST", "/pause")
			pausedAt := time.Now()
			time.Sleep(3 * interval)
			want := committedSum(t, tc.kafka, cons.group, topic)
			d3 := lastDoc(b.waitDocs(a.cluster, topic, cons.group, pausedAt.Add(2*interval), 1))
			if got := d3.num("kafka.lag.consumed_total"); got != want {
				t.Fatalf("sum of committed offsets: agent %v, OffsetFetch %v", got, want)
			}
			t.Logf("%s: stream %v records, unknown keys %v, unknown values %v, fallback %v group(s)", tc.name,
				h.f("kafkabeat.stream.records"), h.f("kafkabeat.stream.unknown_keys"), h.f("kafkabeat.stream.unknown_values"), h.f("kafkabeat.stream.groups_fallback"))
		})
	}
}

// injectUnknownValue écrit dans __consumer_offsets, sur la partition du
// coordinateur du groupe, un commit dont la valeur porte la version 99. Le
// broker n'accepte l'écriture sur un topic interne que du client __admin_client.
func injectUnknownValue(t *testing.T, bootstrap, group, topic string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.ClientID("__admin_client"), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	md, err := kadm.NewClient(cl).Metadata(ctx, "__consumer_offsets")
	if err != nil {
		t.Fatal(err)
	}
	n := int32(len(md.Topics["__consumer_offsets"].Partitions))
	if n == 0 {
		t.Fatal("__consumer_offsets not found")
	}
	k := kmsg.OffsetCommitKey{Version: 1, Group: group, Topic: topic, Partition: 0}
	value := append([]byte{0, 99}, []byte("kafkabeat-recette")...)
	rec := &kgo.Record{Topic: "__consumer_offsets", Partition: javaHashPartition(group, n), Key: k.AppendTo(nil), Value: value}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Skipf("injection refused by the broker (%v): criterion 5 covered by unit tests only", err)
	}
}

// committedSum renvoie la somme des offsets commités d'un groupe sur un topic,
// lue par l'API publique (OffsetFetch).
func committedSum(t *testing.T, bootstrap, group, topic string) float64 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	offs, err := kadm.NewClient(cl).FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	var sum int64
	offs.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic && o.Err == nil {
			sum += o.At
		}
	})
	return float64(sum)
}

// javaHashPartition reproduit Utils.abs(group.hashCode()) % n.
func javaHashPartition(s string, n int32) int32 {
	var h int32
	for _, r := range s {
		h = 31*h + int32(r)
	}
	if h == math.MinInt32 {
		h = 0
	}
	if h < 0 {
		h = -h
	}
	return h % n
}

// Critère 6 : offsets, leader, ISR, membre assigné et métadonnée de commit
// des documents aux noms Metricbeat égaux à ceux de Metricbeat une fois la
// production et la consommation arrêtées.
func TestC6MetricsMatchMetricbeat(t *testing.T) {
	b := newBench(t, kafka39)
	topic := b.topic("c6", 4)
	cons := b.consumer(b.name+"-g6", topic, "--commit", "auto", "--commit-interval", "1s")
	prod, _ := b.producer(topic, "steady:200:10s", false)
	a := b.agent(agentOpts{metricsInterval: 20 * time.Second})
	prod.Wait()
	time.Sleep(5 * time.Second)
	cons.must(t, "POST", "/pause")
	frozenAt := time.Now()
	time.Sleep(70 * time.Second)

	ours := latestPer(search(t, "kafkabeat-metrics", map[string]any{"kafka.cluster.name": a.cluster, "kafka.topic.name": topic,
		"event.dataset": "kafka.partition", "kafka.partition.partition.is_leader": true}, frozenAt.Add(10*time.Second), 1000), "kafka.partition.id")
	theirs := latestPer(search(t, "metricbeat-*", map[string]any{"bench_cluster": "kafka39", "kafka.topic.name": topic,
		"event.dataset": "kafka.partition", "kafka.partition.partition.is_leader": true}, frozenAt.Add(10*time.Second), 1000), "kafka.partition.id")
	if len(ours) != 4 || len(theirs) != 4 {
		t.Fatalf("expected 4 partitions on each side: kafkabeat %d, Metricbeat %d", len(ours), len(theirs))
	}
	for p, o := range ours {
		m := theirs[p]
		for _, k := range []string{"kafka.partition.offset.newest", "kafka.partition.offset.oldest", "kafka.partition.partition.leader", "kafka.partition.partition.replica"} {
			ov, ook := o.numOK(k)
			mv, mok := m.numOK(k)
			if !ook || !mok || ov != mv {
				t.Fatalf("partition %v, %s: kafkabeat %v, Metricbeat %v (present: %v, %v)", p, k, o.f(k), m.f(k), ook, mok)
			}
		}
		if k := "kafka.partition.partition.insync_replica"; o.f(k) == nil || o.boolean(k) != m.boolean(k) {
			t.Fatalf("partition %v, %s: kafkabeat %v, Metricbeat %v", p, k, o.f(k), m.f(k))
		}
	}
	oursCG := latestPer(search(t, "kafkabeat-metrics", map[string]any{"kafka.cluster.name": a.cluster, "kafka.topic.name": topic,
		"event.dataset": "kafka.consumergroup", "kafka.consumergroup.id": cons.group}, frozenAt.Add(10*time.Second), 1000), "kafka.partition.id")
	theirsCG := latestPer(search(t, "metricbeat-*", map[string]any{"bench_cluster": "kafka39", "kafka.topic.name": topic,
		"event.dataset": "kafka.consumergroup", "kafka.consumergroup.id": cons.group}, frozenAt.Add(10*time.Second), 1000), "kafka.partition.id")
	if len(oursCG) != 4 || len(theirsCG) != 4 {
		t.Fatalf("expected 4 consumergroup partitions on each side: kafkabeat %d, Metricbeat %d", len(oursCG), len(theirsCG))
	}
	for p, o := range oursCG {
		m := theirsCG[p]
		for _, k := range []string{"kafka.consumergroup.offset", "kafka.consumergroup.consumer_lag"} {
			ov, ook := o.numOK(k)
			mv, mok := m.numOK(k)
			if !ook || !mok || ov != mv {
				t.Fatalf("partition %v, %s: kafkabeat %v, Metricbeat %v (present: %v, %v)", p, k, o.f(k), m.f(k), ook, mok)
			}
		}
		// consumer en pause mais toujours membre : membre assigné et métadonnée de commit
		for _, k := range []string{"kafka.consumergroup.meta", "kafka.consumergroup.client.id", "kafka.consumergroup.client.host", "kafka.consumergroup.client.member_id"} {
			if o.str(k) == "" || o.str(k) != m.str(k) {
				t.Fatalf("partition %v, %s: kafkabeat %q, Metricbeat %q", p, k, o.str(k), m.str(k))
			}
		}
	}
	t.Logf("criterion 6: partition and consumergroup offsets identical to Metricbeat on %d partitions", len(ours))
}

func latestPer(docs []doc, key string) map[float64]doc {
	out := make(map[float64]doc)
	for _, d := range docs {
		out[d.num(key)] = d // docs triés par @timestamp croissant : le dernier gagne
	}
	return out
}

func countState(docs []doc, state string) int {
	n := 0
	for _, d := range docs {
		if d.str("kafka.lag.state") == state {
			n++
		}
	}
	return n
}

func head(xs []string, n int) []string {
	if len(xs) > n {
		return append(xs[:n], fmt.Sprintf("… and %d more", len(xs)-n))
	}
	return xs
}
