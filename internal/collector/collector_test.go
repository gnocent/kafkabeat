package collector

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/gnocent/kafkabeat/internal/config"
	"github.com/gnocent/kafkabeat/internal/history"
	"github.com/gnocent/kafkabeat/internal/kafkaclient"
	"github.com/gnocent/kafkabeat/internal/sink"
	"github.com/twmb/franz-go/pkg/kerr"
)

type fakeBroker struct {
	md            kafkaclient.Metadata
	mdErr         error
	mdWait        bool // Metadata attend l'annulation du contexte (cycle trop long)
	leo           kafkaclient.Offsets
	groups        map[string]kafkaclient.Group
	groupsPartial []error          // brokers en échec de ListGroups
	unavailable   map[string]error // groupes dont le coordinateur ne répond pas (DescribeGroups)
	commits       kafkaclient.GroupCommits
	groupErr      map[string]error
	start         kafkaclient.Offsets
	members       map[string]kafkaclient.GroupView
}

func (f *fakeBroker) Metadata(ctx context.Context) (kafkaclient.Metadata, error) {
	if f.mdWait {
		<-ctx.Done()
	}
	if err := ctx.Err(); err != nil {
		return kafkaclient.Metadata{}, err
	}
	return f.md, f.mdErr
}
func (f *fakeBroker) EndOffsets(_ context.Context, _ []string) (kafkaclient.Offsets, []error, error) {
	return f.leo, nil, nil
}
func (f *fakeBroker) StartOffsets(_ context.Context, _ []string) (kafkaclient.Offsets, []error, error) {
	return f.start, nil, nil
}
func (f *fakeBroker) ListGroups(context.Context) (map[string]kafkaclient.Group, []error, error) {
	return f.groups, f.groupsPartial, nil
}
func (f *fakeBroker) GroupsUnavailable(_ context.Context, groups []string) map[string]error {
	out := map[string]error{}
	for _, g := range groups {
		if err, ok := f.unavailable[g]; ok {
			out[g] = err
		}
	}
	return out
}
func (f *fakeBroker) Members(context.Context, []string) (map[string]kafkaclient.GroupView, error) {
	return f.members, nil
}
func (f *fakeBroker) Commits(_ context.Context, groups []string) (kafkaclient.GroupCommits, map[string]error, error) {
	// comme OffsetFetch : seuls les groupes demandés (listés et retenus) sont renvoyés
	out := make(kafkaclient.GroupCommits, len(groups))
	for _, g := range groups {
		if topics, ok := f.commits[g]; ok {
			out[g] = topics
		}
	}
	return out, f.groupErr, nil
}
func (f *fakeBroker) Mode() string { return "fake" }

type memSink struct {
	docs  []sink.Doc
	delay time.Duration // temps d'émission simulé des documents (hors santé)
}

func (m *memSink) Emit(docs []sink.Doc) error {
	if m.delay > 0 && len(docs) > 0 && docs[0].Stream != "health" {
		time.Sleep(m.delay)
	}
	m.docs = append(m.docs, docs...)
	return nil
}

func (m *memSink) lastByStream(stream string) []sink.Doc {
	var out []sink.Doc
	for _, d := range m.docs {
		if d.Stream == stream {
			out = append(out, d)
		}
	}
	return out
}

var t0 = time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)

func newTest(t *testing.T) (*Collector, *fakeBroker, *memSink) {
	t.Helper()
	cfg := config.Default()
	cfg.Cluster = "c"
	cfg.Output.Fields = map[string]string{"site": "lab"}
	fb := &fakeBroker{
		md: kafkaclient.Metadata{ClusterID: "cid", Topics: map[string]kafkaclient.TopicMeta{
			"t":        {Name: "t", ID: "abc", Partitions: []kafkaclient.PartitionMeta{{ID: 0}, {ID: 1}, {ID: 2}}},
			"__hidden": {Name: "__hidden", Internal: true, Partitions: []kafkaclient.PartitionMeta{{ID: 0}}},
		}},
		leo:     kafkaclient.Offsets{"t": {0: 100, 1: 100, 2: 100}},
		groups:  map[string]kafkaclient.Group{"g": {State: "Stable"}, "skip": {}},
		commits: kafkaclient.GroupCommits{},
	}
	cfg.Collect.Groups.Exclude = []string{"skip"}
	ms := &memSink{}
	col := New(cfg, fb, fb, history.New(HistoryTiers(cfg.History)), ms, log.New(os.Stderr, "", 0), "test")
	return col, fb, ms
}

func lagOf(t *testing.T, d sink.Doc) map[string]any {
	t.Helper()
	return d.Body["kafka"].(map[string]any)["lag"].(map[string]any)
}

func TestCycleBrackets(t *testing.T) {
	col, fb, ms := newTest(t)
	// cycle 1 : LEO 100 partout, groupe à jour sur p0, en retard sur p1, sans commit sur p2
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	fb.commits.Set("g", "t", 1, kafkaclient.Commit{Offset: 90})
	col.Cycle(context.Background(), t0)
	docs := ms.lastByStream("lag")
	if len(docs) != 1 {
		t.Fatalf("1 document de lag attendu, %d reçus", len(docs))
	}
	lag := lagOf(t, docs[0])
	if lag["offsets_sum"].(int64) != 10 || lag["partitions_lagging"].(int) != 1 || lag["partitions_no_commit"].(int) != 1 {
		t.Fatalf("agrégats inattendus : %+v", lag)
	}
	if lag["method"] != "bracket_open" || lag["seconds_upper"] != nil {
		t.Fatalf("avant toute hausse du LEO l'encadrement doit être ouvert : %+v", lag)
	}
	if docs[0].Body["site"] != "lab" {
		t.Fatal("enrichissement statique absent")
	}

	// cycle 2 : LEO monte à 200 ; l'offset 150 commité par p1 date de ce cycle
	fb.leo["t"] = map[int32]int64{0: 200, 1: 200, 2: 200}
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 200})
	fb.commits.Set("g", "t", 1, kafkaclient.Commit{Offset: 150})
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	lag = lagOf(t, ms.lastByStream("lag")[1])
	if lag["method"] != "bracket" || lag["seconds_lower"].(float64) != 0 || lag["seconds_upper"].(float64) != 10 {
		t.Fatalf("encadrement attendu [0, 10] : %+v", lag)
	}
	if lag["worst_partition_time"].(int32) != 1 || lag["worst_partition_offsets"].(int32) != 1 || lag["partitions_lagging"].(int) != 1 {
		t.Fatalf("pires partitions inattendues : %+v", lag)
	}

	// cycle 3 : commit figé, LEO stable : la borne haute avance de 10 s
	col.Cycle(context.Background(), t0.Add(20*time.Second))
	lag = lagOf(t, ms.lastByStream("lag")[2])
	if lag["seconds_lower"].(float64) != 10 || lag["seconds_upper"].(float64) != 20 || lag["last_commit_age_max_s"].(float64) != 10 {
		t.Fatalf("encadrement attendu [10, 20], âge du commit 10 : %+v", lag)
	}
	if h := ms.lastByStream("health"); len(h) != 3 {
		t.Fatalf("un document de santé par cycle attendu, %d reçus", len(h))
	}
}

func TestCycleMetadataFailure(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.mdErr = errors.New("boom")
	col.Cycle(context.Background(), t0)
	if len(ms.lastByStream("lag")) != 0 {
		t.Fatal("aucun document de lag ne doit être émis sans inventaire")
	}
	h := ms.lastByStream("health")
	if len(h) != 1 {
		t.Fatalf("un document de santé attendu, %d reçus", len(h))
	}
	kb := h[0].Body["kafkabeat"].(map[string]any)
	if kb["cycle_incomplete"] != true || len(kb["errors"].([]string)) != 1 {
		t.Fatalf("le document de santé doit porter l'erreur : %+v", kb)
	}
}

func TestListGroupsPartialFailureKeepsPairs(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	col.Cycle(context.Background(), t0)
	// cycle 2 : le coordinateur de g est injoignable, ListGroups ne le liste plus
	listed := fb.groups
	fb.groups, fb.groupsPartial = map[string]kafkaclient.Group{}, []error{errors.New("broker 2 : boom")}
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	if n := len(ms.lastByStream("lag")); n != 1 {
		t.Fatalf("aucun document gone attendu pendant une liste incomplète, %d document(s) de lag", n)
	}
	kb := ms.lastByStream("health")[1].Body["kafkabeat"].(map[string]any)
	if kb["pairs_unknown"] != 1 || kb["cycle_incomplete"] != true || kb["partial_errors"] != 1 {
		t.Fatalf("paire inconnue et cycle incomplet attendus : %+v", kb)
	}
	// cycle 3 : liste complète, la paire reprend sans passer par gone ni group_recreated
	fb.groups, fb.groupsPartial = listed, nil
	col.Cycle(context.Background(), t0.Add(20*time.Second))
	lag := lagOf(t, ms.lastByStream("lag")[1])
	if lag["state"] == StateGone || flagsOf(t, lag)[flagGroupRecr] || lag["interval_ms"].(int64) != 20000 {
		t.Fatalf("paire reprise sans gone, références de taux gardées : %+v", lag)
	}
}

func TestCoordinatorLoadingKeepsPairs(t *testing.T) {
	// reprise d'un broker : ListGroups répond une liste vide sans erreur tant
	// que le coordinateur recharge ses groupes ; DescribeGroups le dit
	col, fb, ms := newTest(t)
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	col.Cycle(context.Background(), t0)
	listed := fb.groups
	fb.groups, fb.unavailable = map[string]kafkaclient.Group{}, map[string]error{"g": kerr.CoordinatorLoadInProgress}
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	if n := len(ms.lastByStream("lag")); n != 1 {
		t.Fatalf("aucun document gone attendu pendant le chargement du coordinateur, %d document(s) de lag", n)
	}
	kb := ms.lastByStream("health")[1].Body["kafkabeat"].(map[string]any)
	if kb["pairs_unknown"] != 1 || kb["cycle_incomplete"] != true || kb["group_errors"] != 1 {
		t.Fatalf("paire inconnue et cycle incomplet attendus : %+v", kb)
	}
	fb.groups, fb.unavailable = listed, nil
	col.Cycle(context.Background(), t0.Add(20*time.Second))
	lag := lagOf(t, ms.lastByStream("lag")[1])
	if lag["state"] == StateGone || flagsOf(t, lag)[flagGroupRecr] || lag["interval_ms"].(int64) != 20000 {
		t.Fatalf("paire reprise sans gone ni group_recreated : %+v", lag)
	}
	// groupe réellement supprimé : DescribeGroups ne signale rien, la paire passe gone
	fb.groups = map[string]kafkaclient.Group{}
	col.Cycle(context.Background(), t0.Add(30*time.Second))
	if lag := lagOf(t, ms.lastByStream("lag")[2]); lag["state"] != StateGone {
		t.Fatalf("groupe supprimé : gone attendu : %+v", lag)
	}
}

func TestCycleDurationIncludesEmission(t *testing.T) {
	col, fb, ms := newTest(t)
	ms.delay = 50 * time.Millisecond
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	col.Cycle(context.Background(), t0)
	if last := ms.docs[len(ms.docs)-1]; last.Stream != "health" {
		t.Fatalf("le document de santé doit partir en dernier, reçu %s", last.Stream)
	}
	kb := ms.lastByStream("health")[0].Body["kafkabeat"].(map[string]any)
	if d := kb["cycle_duration_ms"].(int64); d < 50 {
		t.Fatalf("la durée du cycle doit inclure l'émission des documents (50 ms simulés) : %d ms", d)
	}
}

func TestCycleInterruptedByShutdown(t *testing.T) {
	// arrêt de l'agent pendant un appel Kafka : ni document de santé cycle_failed, ni erreur
	col, _, ms := newTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	col.Cycle(ctx, t0)
	if len(ms.docs) != 0 {
		t.Fatalf("un cycle interrompu par l'arrêt ne doit rien publier, %d document(s) reçus", len(ms.docs))
	}
	// cycle trop long (trois intervalles), agent vivant : panne signalée comme avant
	col, fb, ms := newTest(t)
	col.cfg.Collect.Interval = time.Millisecond
	fb.mdWait = true
	col.Cycle(context.Background(), t0)
	h := ms.lastByStream("health")
	if len(h) != 1 || h[0].Body["kafkabeat"].(map[string]any)["cycle_failed"] != true {
		t.Fatalf("un cycle abandonné sur délai doit publier cycle_failed : %+v", h)
	}
}

func TestFilters(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.commits.Set("skip", "t", 0, kafkaclient.Commit{Offset: 1})
	fb.commits.Set("g", "__hidden", 0, kafkaclient.Commit{Offset: 1})
	fb.commits.Set("g", "deleted", 0, kafkaclient.Commit{Offset: 1})
	fb.leo["__hidden"] = map[int32]int64{0: 5}
	col.Cycle(context.Background(), t0)
	if n := len(ms.lastByStream("lag")); n != 0 {
		t.Fatalf("groupe exclu, topic interne et topic absent ne doivent produire aucun document, %d reçus", n)
	}
	internal := false
	for _, d := range ms.lastByStream("metrics") {
		if d.Body["kafka"].(map[string]any)["topic"].(map[string]any)["name"] == "__hidden" {
			internal = true
		}
	}
	if !internal {
		t.Fatal("les topics internes doivent avoir leurs documents partition, comme dans Metricbeat")
	}
}

func TestTopicOwner(t *testing.T) {
	col, fb, ms := newTest(t)
	col.cfg.Output.Owners = []config.Owner{{Topic: "x*", Owner: "autre"}, {Topic: "t", Owner: "equipe"}}
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	col.Cycle(context.Background(), t0)
	owners := map[string]map[any]bool{}
	for _, d := range append(ms.lastByStream("lag"), ms.lastByStream("metrics")...) {
		topic := d.Body["kafka"].(map[string]any)["topic"].(map[string]any)
		ds := d.Body["event"].(map[string]any)["dataset"].(string)
		if owners[ds] == nil {
			owners[ds] = map[any]bool{}
		}
		owner, ok := topic["owner"]
		if topic["name"] == "__hidden" {
			if ok {
				t.Fatalf("aucune règle ne correspond à __hidden, owner absent attendu : %+v", topic)
			}
			continue
		}
		owners[ds][owner] = true
	}
	for _, ds := range []string{"kafkabeat.lag", "kafka.partition", "kafka.consumergroup"} {
		if len(owners[ds]) != 1 || !owners[ds]["equipe"] {
			t.Fatalf("%s : owner « equipe » attendu, reçus %v", ds, owners[ds])
		}
	}
}
