package collector

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/gnocent/kafkabeat/internal/history"
	"github.com/gnocent/kafkabeat/internal/kafkaclient"
)

func flagsOf(t *testing.T, lag map[string]any) map[string]bool {
	t.Helper()
	return lag["flags"].(map[string]bool)
}

func TestStatesStalledAndRates(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	fb.commits.Set("g", "t", 1, kafkaclient.Commit{Offset: 50})
	fb.commits.Set("g", "t", 2, kafkaclient.Commit{Offset: 100})
	col.Cycle(context.Background(), t0)
	lag := lagOf(t, ms.lastByStream("lag")[0])
	if lag["state"] != StateLagging || lag["produce_rate"] != nil || lag["interval_ms"] != nil {
		t.Fatalf("premier cycle : lagging sans taux attendu : %+v", lag)
	}
	// 10 s plus tard : production +300 (100 par partition), consommation +50 sur p1 seulement
	fb.leo["t"] = map[int32]int64{0: 200, 1: 200, 2: 200}
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 200})
	fb.commits.Set("g", "t", 1, kafkaclient.Commit{Offset: 100})
	fb.commits.Set("g", "t", 2, kafkaclient.Commit{Offset: 200})
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	lag = lagOf(t, ms.lastByStream("lag")[1])
	if lag["produce_rate"].(float64) != 30 || lag["consume_rate"].(float64) != 25 || lag["interval_ms"].(int64) != 10000 {
		t.Fatalf("taux attendus 30 et 25 : %+v", lag)
	}
	if lag["consume_rate_smoothed"].(float64) != 25 {
		t.Fatalf("taux lissé attendu 25 : %+v", lag)
	}
	// commit figé pendant 60 s avec du lag : stalled
	for i := 2; i <= 7; i++ {
		col.Cycle(context.Background(), t0.Add(time.Duration(i)*10*time.Second))
	}
	docs := ms.lastByStream("lag")
	lag = lagOf(t, docs[len(docs)-1])
	if lag["state"] != StateStalled || lag["partitions_stalled"].(int) != 1 || lag["consume_rate"].(float64) != 0 {
		t.Fatalf("stalled attendu après 60 s sans commit : %+v", lag)
	}
	// rattrapage complet : caught_up
	fb.commits.Set("g", "t", 1, kafkaclient.Commit{Offset: 200})
	col.Cycle(context.Background(), t0.Add(80*time.Second))
	docs = ms.lastByStream("lag")
	if lag = lagOf(t, docs[len(docs)-1]); lag["state"] != StateCaughtUp {
		t.Fatalf("caught_up attendu : %+v", lag)
	}
}

func TestStalledOnFrozenOffsetDespiteRecommits(t *testing.T) {
	// flux : le consumer recommite le même offset à chaque cycle, vivant mais bloqué
	col, fb, ms := newTest(t)
	for p := int32(0); p < 3; p++ {
		fb.commits.Set("g", "t", p, kafkaclient.Commit{Offset: 100, At: t0})
	}
	fb.commits.Set("g", "t", 1, kafkaclient.Commit{Offset: 50, At: t0})
	col.Cycle(context.Background(), t0)
	for i := 1; i <= 6; i++ {
		now := t0.Add(time.Duration(i) * 10 * time.Second)
		fb.commits.Set("g", "t", 1, kafkaclient.Commit{Offset: 50, At: now})
		col.Cycle(context.Background(), now)
	}
	docs := ms.lastByStream("lag")
	if lag := lagOf(t, docs[len(docs)-2]); lag["state"] != StateLagging {
		t.Fatalf("offset figé depuis 50 s : lagging attendu : %+v", lag)
	}
	lag := lagOf(t, docs[len(docs)-1])
	if lag["state"] != StateStalled || lag["partitions_stalled"].(int) != 1 {
		t.Fatalf("offset figé depuis 60 s malgré les recommits : stalled attendu : %+v", lag)
	}
	// l'âge publié reste celui du dernier commit, daté par le flux
	if lag["last_commit_age_max_s"].(float64) != 0 {
		t.Fatalf("dernier commit à l'instant : âge 0 attendu : %+v", lag)
	}
}

func TestSmoothedRateWindow(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.leo["t"] = map[int32]int64{0: 1000, 1: 1000, 2: 1000}
	var commit int64
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: commit})
	col.Cycle(context.Background(), t0)
	// dix cycles de 10 s où le groupe consomme 1, 2, ..., 10 messages
	for i := int64(1); i <= 10; i++ {
		commit += i
		fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: commit})
		col.Cycle(context.Background(), t0.Add(time.Duration(i)*10*time.Second))
	}
	docs := ms.lastByStream("lag")
	lag := lagOf(t, docs[len(docs)-1])
	// fenêtre de 60 s : les six derniers échantillons, 5+6+7+8+9+10 = 45 messages
	if lag["consume_rate"].(float64) != 1 || lag["consume_rate_smoothed"].(float64) != 0.75 {
		t.Fatalf("taux instantané 1/s et lissé 0,75/s attendus : %+v", lag)
	}
}

func TestRatesSkippedWhenEndOffsetMissing(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	col.Cycle(context.Background(), t0)
	// cycle 2 : la partition 2 n'a pas d'offset de fin (erreur partielle) : aucun taux, pas de débit négatif
	fb.leo["t"] = map[int32]int64{0: 100, 1: 100}
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	lag := lagOf(t, ms.lastByStream("lag")[1])
	if lag["partitions_unknown"].(int) != 1 || lag["produce_rate"] != nil || lag["consume_rate"] != nil || lag["interval_ms"] != nil {
		t.Fatalf("partition sans offset de fin : aucun taux attendu : %+v", lag)
	}
	// cycle 3 complet : taux depuis la dernière référence complète, 20 s plus tôt
	fb.leo["t"] = map[int32]int64{0: 130, 1: 130, 2: 130}
	col.Cycle(context.Background(), t0.Add(20*time.Second))
	lag = lagOf(t, ms.lastByStream("lag")[2])
	if lag["produce_rate"].(float64) != 4.5 || lag["consume_rate"].(float64) != 0 || lag["interval_ms"].(int64) != 20000 {
		t.Fatalf("taux produit 4,5/s sur 20 s attendu : %+v", lag)
	}
}

func TestProductionStatus(t *testing.T) {
	col, fb, ms := newTest(t)
	for p := int32(0); p < 3; p++ {
		fb.commits.Set("g", "t", p, kafkaclient.Commit{Offset: 100})
	}
	production := func() (any, float64) {
		docs := ms.lastByStream("lag")
		lag := lagOf(t, docs[len(docs)-1])
		return lag["production"], lag["last_produce_age_s"].(float64)
	}
	// premier cycle : aucune avance vue, statut inconnu
	col.Cycle(context.Background(), t0)
	if s, age := production(); s != nil || age != 0 {
		t.Fatalf("premier cycle : statut inconnu attendu, %v %v", s, age)
	}
	// une seule partition reçoit des messages : le topic produit
	fb.leo["t"] = map[int32]int64{0: 100, 1: 150, 2: 100}
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	if s, age := production(); s != ProductionActive || age != 0 {
		t.Fatalf("production active attendue : %v %v", s, age)
	}
	// plus aucun message pendant stalled_after (60 s) : arrêtée
	for i := 2; i <= 6; i++ {
		col.Cycle(context.Background(), t0.Add(time.Duration(i)*10*time.Second))
	}
	if s, age := production(); s != ProductionActive || age != 50 {
		t.Fatalf("50 s sans message : encore active attendue, %v %v", s, age)
	}
	col.Cycle(context.Background(), t0.Add(70*time.Second))
	if s, age := production(); s != ProductionStopped || age != 60 {
		t.Fatalf("60 s sans message : production arrêtée attendue, %v %v", s, age)
	}
}

func TestOffsetResetFlag(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	fb.commits.Set("g", "t", 1, kafkaclient.Commit{Offset: 100})
	fb.commits.Set("g", "t", 2, kafkaclient.Commit{Offset: 100})
	col.Cycle(context.Background(), t0)
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	fb.commits.Set("g", "t", 1, kafkaclient.Commit{Offset: 0})
	col.Cycle(context.Background(), t0.Add(20*time.Second))
	docs := ms.lastByStream("lag")
	lag := lagOf(t, docs[2])
	if !flagsOf(t, lag)[flagOffsetReset] || lag["consume_rate"] != nil {
		t.Fatalf("reset : flag levé et taux nul attendus : %+v", lag)
	}
	if flagsOf(t, lagOf(t, docs[1]))[flagOffsetReset] {
		t.Fatal("le flag ne doit pas être levé avant le reset")
	}
	for i := 3; i < 3+col.cfg.Collect.FlagHoldCycles; i++ {
		col.Cycle(context.Background(), t0.Add(time.Duration(i)*10*time.Second))
	}
	docs = ms.lastByStream("lag")
	if flagsOf(t, lagOf(t, docs[len(docs)-2]))[flagOffsetReset] == false {
		t.Fatal("le flag doit rester levé pendant flag_hold_cycles")
	}
	if flagsOf(t, lagOf(t, docs[len(docs)-1]))[flagOffsetReset] {
		t.Fatal("le flag doit retomber après flag_hold_cycles")
	}
}

func TestGoneThenPurgeAndRecreated(t *testing.T) {
	col, fb, ms := newTest(t)
	col.cfg.Collect.GoneCycles = 2
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	col.Cycle(context.Background(), t0)
	delete(fb.commits, "g")
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	col.Cycle(context.Background(), t0.Add(20*time.Second))
	col.Cycle(context.Background(), t0.Add(30*time.Second))
	docs := ms.lastByStream("lag")
	if len(docs) != 3 || lagOf(t, docs[1])["state"] != StateGone || lagOf(t, docs[2])["state"] != StateGone {
		t.Fatalf("2 documents gone puis purge attendus : %d documents", len(docs))
	}
	if len(col.pairs) != 0 {
		t.Fatal("la paire doit être purgée")
	}
	// disparition puis retour avant purge : group_recreated
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	col.Cycle(context.Background(), t0.Add(40*time.Second))
	delete(fb.commits, "g")
	col.Cycle(context.Background(), t0.Add(50*time.Second))
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	col.Cycle(context.Background(), t0.Add(60*time.Second))
	docs = ms.lastByStream("lag")
	// p1 et p2 n'ont pas de commit : no_commit prime sur caught_up
	if lag := lagOf(t, docs[len(docs)-1]); lag["state"] != StateNoCommit || !flagsOf(t, lag)[flagGroupRecr] {
		t.Fatalf("retour avant purge : no_commit et group_recreated attendus : %+v", lag)
	}
}

func TestUnknownGroupKeepsState(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	col.Cycle(context.Background(), t0)
	delete(fb.commits, "g")
	fb.groupErr = map[string]error{"g": errors.New("coordinator down")}
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	if n := len(ms.lastByStream("lag")); n != 1 {
		t.Fatalf("un groupe en erreur ne doit pas passer gone : %d documents", n)
	}
	h := ms.lastByStream("health")[1].Body["kafkabeat"].(map[string]any)
	if h["pairs_unknown"].(int) != 1 || h["cycle_incomplete"] != true {
		t.Fatalf("santé : paire inconnue et cycle incomplet attendus : %+v", h)
	}
}

func TestTopicRecreatedAndTruncation(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 50})
	col.Cycle(context.Background(), t0)
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	tm := fb.md.Topics["t"]
	tm.ID = "new-id"
	fb.md.Topics["t"] = tm
	fb.leo["t"] = map[int32]int64{0: 10, 1: 10, 2: 10}
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 0})
	col.Cycle(context.Background(), t0.Add(20*time.Second))
	docs := ms.lastByStream("lag")
	f := flagsOf(t, lagOf(t, docs[2]))
	// recréation : l'historique est oublié avant l'observation, donc pas de troncature signalée
	if !f[flagTopicRecr] || !f[flagOffsetReset] || f[flagLogTruncated] {
		t.Fatalf("topic recréé et reset attendus, sans troncature : %+v", f)
	}
	if e := col.hist.Entries(history.Key{Topic: "t", Partition: 0}); len(e) != 1 || e[0].Leo != 10 {
		t.Fatalf("l'historique doit repartir de zéro : %+v", e)
	}
	// recul du LEO sans changement d'identifiant : troncature
	fb.leo["t"] = map[int32]int64{0: 5, 1: 10, 2: 10}
	col.Cycle(context.Background(), t0.Add(30*time.Second))
	docs = ms.lastByStream("lag")
	if f := flagsOf(t, lagOf(t, docs[3])); !f[flagLogTruncated] {
		t.Fatalf("troncature attendue : %+v", f)
	}
}

func TestSnapshotRestoreAndGap(t *testing.T) {
	col, fb, ms := newTest(t)
	dir := t.TempDir()
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 50})
	col.Cycle(context.Background(), t0)
	fb.leo["t"] = map[int32]int64{0: 200, 1: 200, 2: 200}
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 150})
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	if err := col.Save(dir, t0.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	// redémarrage après 5 minutes d'arrêt
	cfg := col.cfg
	restart := New(cfg, fb, fb, history.New(HistoryTiers(cfg.History)), ms, log.New(os.Stderr, "", 0), "test")
	savedAt, err := restart.Load(dir, t0.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !savedAt.Equal(t0.Add(10*time.Second)) || restart.cycle != 2 || len(restart.pairs) != 1 {
		t.Fatalf("état non restauré : savedAt=%v cycle=%d pairs=%d", savedAt, restart.cycle, len(restart.pairs))
	}
	if _, entries := restart.hist.Stats(); entries == 0 {
		t.Fatal("historique non restauré")
	}
	restart.Cycle(context.Background(), t0.Add(5*time.Minute))
	docs := ms.lastByStream("lag")
	lag := lagOf(t, docs[len(docs)-1])
	if !flagsOf(t, lag)[flagResumedGap] || lag["produce_rate"] != nil {
		t.Fatalf("après un arrêt long : resumed_after_gap et pas de taux : %+v", lag)
	}
	// les bornes reprennent depuis le disque : l'offset 150 a été produit entre t0 et t0+10 s
	if lag["seconds_lower"].(float64) != 290 || lag["seconds_upper"].(float64) != 300 {
		t.Fatalf("bornes attendues [290, 300] pour l'offset 150 produit entre t0 et t0+10 s : %+v", lag)
	}
	// pas de fichier : départ de zéro sans erreur
	if _, err := New(cfg, fb, fb, history.New(nil), ms, log.New(os.Stderr, "", 0), "test").Load(t.TempDir(), t0); err != nil {
		t.Fatal(err)
	}
}

// Kafka efface les offsets d'un topic supprimé : au cycle où le nouvel
// identifiant apparaît, la paire n'a souvent plus de commit. Le flag la suit
// quand même : document gone, puis document de lag à son retour.
func TestTopicRecreatedWhilePairAbsent(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 50})
	col.Cycle(context.Background(), t0)
	tm := fb.md.Topics["t"]
	tm.ID = "new-id"
	fb.md.Topics["t"] = tm
	fb.leo["t"] = map[int32]int64{0: 10, 1: 10, 2: 10}
	delete(fb.commits, "g")
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 5})
	col.Cycle(context.Background(), t0.Add(20*time.Second))
	docs := ms.lastByStream("lag")
	if len(docs) != 3 || lagOf(t, docs[1])["state"] != StateGone {
		t.Fatalf("lag, gone puis lag attendus : %d documents", len(docs))
	}
	for i, d := range docs[1:] {
		if !flagsOf(t, lagOf(t, d))[flagTopicRecr] {
			t.Fatalf("document %d après la recréation : topic_recreated attendu : %+v", i+1, lagOf(t, d))
		}
	}
}

// Un cycle métriques entre la suppression et la recréation ne doit pas faire
// oublier l'identifiant du topic tant qu'une paire le connaît.
func TestTopicRecreatedAcrossMetricsCycle(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 50})
	col.Cycle(context.Background(), t0)
	tm := fb.md.Topics["t"]
	delete(fb.md.Topics, "t")
	delete(fb.leo, "t")
	delete(fb.commits, "g")
	col.Cycle(context.Background(), t0.Add(60*time.Second))
	tm.ID = "new-id"
	fb.md.Topics["t"] = tm
	fb.leo["t"] = map[int32]int64{0: 10, 1: 10, 2: 10}
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 5})
	col.Cycle(context.Background(), t0.Add(70*time.Second))
	docs := ms.lastByStream("lag")
	if f := flagsOf(t, lagOf(t, docs[len(docs)-1])); !f[flagTopicRecr] {
		t.Fatalf("topic recréé de part et d'autre d'un cycle métriques : topic_recreated attendu : %+v", f)
	}
}
