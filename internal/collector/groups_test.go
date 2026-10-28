package collector

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/gnocent/kafkabeat/internal/history"
	"github.com/gnocent/kafkabeat/internal/kafkaclient"
	"github.com/gnocent/kafkabeat/internal/sink"
)

// fakeStream ajoute au faux broker les métadonnées de groupe du flux.
type fakeStream struct {
	*fakeBroker
	views    []kafkaclient.GroupView
	fallback map[string]bool
	healthy  bool
}

func (f *fakeStream) Groups() ([]kafkaclient.GroupView, map[string]bool, bool) {
	v := f.views
	f.views = nil
	return v, f.fallback, f.healthy
}

func newStreamTest(t *testing.T) (*Collector, *fakeStream, *memSink) {
	t.Helper()
	col, fb, ms := newTest(t)
	fs := &fakeStream{fakeBroker: fb, healthy: true}
	col = New(col.cfg, fb, fs, history.New(HistoryTiers(col.cfg.History)), ms, log.New(os.Stderr, "", 0), "test")
	return col, fs, ms
}

func member(id string, parts ...int32) kafkaclient.Member {
	return kafkaclient.Member{ID: id, ClientID: "app-" + id, ClientHost: "10.0.0.1", Topics: []string{"t"},
		Assigned: map[string][]int32{"t": parts}}
}

func described(state string, members ...kafkaclient.Member) map[string]kafkaclient.GroupView {
	return map[string]kafkaclient.GroupView{"g": {Group: "g", Generation: -1, ProtocolType: "consumer", Protocol: "range",
		State: state, Members: members}}
}

func streamed(gen int32, at time.Time, members ...kafkaclient.Member) kafkaclient.GroupView {
	return kafkaclient.GroupView{Group: "g", Generation: gen, ProtocolType: "consumer", Protocol: "range", At: at, Members: members}
}

func rebalanceOf(d sink.Doc) map[string]any {
	return d.Body["kafka"].(map[string]any)["rebalance"].(map[string]any)
}

func groupOf(d sink.Doc) map[string]any {
	return d.Body["kafka"].(map[string]any)["consumergroup"].(map[string]any)
}

func ids(list any) []string {
	var out []string
	for _, m := range list.([]map[string]any) {
		out = append(out, m["member_id"].(string))
	}
	return out
}

func TestRebalancesFromStream(t *testing.T) {
	col, fs, ms := newStreamTest(t)
	fs.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100})
	// cycle 1 : référence DescribeGroups, aucun rebalance
	fs.members = described("Stable", member("m1", 0, 1, 2))
	col.Cycle(context.Background(), t0)
	if n := len(ms.lastByStream("events")); n != 0 {
		t.Fatalf("la référence ne doit pas produire de rebalance, %d reçu(s)", n)
	}
	if cg := groupOf(ms.lastByStream("lag")[0]); cg["members"] != 1 || cg["assignment"] != AssignmentMembers {
		t.Fatalf("1 membre attendu : %+v", cg)
	}

	// m2 entre (génération 5) ; puis le groupe est vu en rebalance et m1 sort (génération 6)
	fs.views = []kafkaclient.GroupView{streamed(5, t0.Add(5*time.Second), member("m1", 0, 1), member("m2", 2))}
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	fs.groups["g"] = kafkaclient.Group{State: "PreparingRebalance"}
	col.Cycle(context.Background(), t0.Add(20*time.Second))
	fs.groups["g"] = kafkaclient.Group{State: "Stable"}
	fs.views = []kafkaclient.GroupView{streamed(6, t0.Add(25*time.Second), member("m2", 0, 1, 2))}
	col.Cycle(context.Background(), t0.Add(30*time.Second))

	ev := ms.lastByStream("events")
	if len(ev) != 2 {
		t.Fatalf("2 rebalances attendus, %d reçus", len(ev))
	}
	r1, r2 := rebalanceOf(ev[0]), rebalanceOf(ev[1])
	if r1["method"] != MethodStream || groupOf(ev[0])["generation"] != int32(5) || r1["previous_generation"] != nil ||
		len(ids(r1["joined"])) != 1 || ids(r1["joined"])[0] != "m2" || r1["left"] != nil || r1["duration_s"] != nil {
		t.Fatalf("génération 5 : m2 entré attendu : %+v", r1)
	}
	if ev[0].Body["@timestamp"] != t0.Add(5*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("horodatage du rebalance attendu : %v", ev[0].Body["@timestamp"])
	}
	if r2["previous_generation"] != int32(5) || ids(r2["left"])[0] != "m1" || r2["joined"] != nil || r2["duration_s"] != 5.0 {
		t.Fatalf("génération 6 : m1 sorti, durée 5 s attendus : %+v", r2)
	}
	if topic := ev[1].Body["kafka"].(map[string]any)["topic"].(map[string]any); topic["name"].([]string)[0] != "t" {
		t.Fatalf("topics abonnés attendus : %+v", topic)
	}

	// cycle métriques : DescribeGroups ne remplace pas la vue du flux ; un
	// enregistrement rejoué de la même génération n'est pas un rebalance
	fs.members = described("Stable", member("m9", 0, 1, 2))
	fs.views = []kafkaclient.GroupView{streamed(6, t0.Add(25*time.Second), member("m2", 0, 1, 2))}
	col.Cycle(context.Background(), t0.Add(60*time.Second))
	if n := len(ms.lastByStream("events")); n != 2 {
		t.Fatalf("aucun nouveau rebalance attendu, total %d", n)
	}
	if h := ms.lastByStream("health"); h[len(h)-1].Body["kafkabeat"].(map[string]any)["rebalance_method"] != MethodStream {
		t.Fatal("méthode stream attendue dans la santé")
	}
}

func TestRebalancesDescribeFallback(t *testing.T) {
	col, fb, ms := newTest(t) // mode polling : pas de flux
	fb.members = described("Stable", member("m1", 0, 1, 2))
	col.Cycle(context.Background(), t0)
	col.Cycle(context.Background(), t0.Add(60*time.Second))
	fb.members = described("Stable", member("m1", 0, 1), member("m2", 2))
	col.Cycle(context.Background(), t0.Add(70*time.Second)) // hors échéance métriques : rien vu
	if n := len(ms.lastByStream("events")); n != 0 {
		t.Fatalf("aucun rebalance attendu sans changement vu, %d reçu(s)", n)
	}
	col.Cycle(context.Background(), t0.Add(120*time.Second))
	ev := ms.lastByStream("events")
	if len(ev) != 1 {
		t.Fatalf("1 rebalance attendu, %d reçus", len(ev))
	}
	r := rebalanceOf(ev[0])
	if r["method"] != MethodDescribe || groupOf(ev[0])["generation"] != nil || ids(r["joined"])[0] != "m2" {
		t.Fatalf("repli describe attendu : %+v %+v", r, groupOf(ev[0]))
	}
	if h := ms.lastByStream("health"); h[len(h)-1].Body["kafkabeat"].(map[string]any)["rebalance_method"] != MethodDescribe {
		t.Fatal("méthode describe attendue dans la santé")
	}
}

func TestRebalancesStreamOutage(t *testing.T) {
	col, fs, ms := newStreamTest(t)
	fs.members = described("Stable", member("m1", 0, 1, 2))
	col.Cycle(context.Background(), t0)
	// flux en erreur : DescribeGroups fait foi et voit m2 entrer
	fs.healthy = false
	fs.members = described("Stable", member("m1", 0, 1), member("m2", 2))
	v := fs.members["g"]
	v.At = t0.Add(60 * time.Second)
	fs.members["g"] = v
	col.Cycle(context.Background(), t0.Add(60*time.Second))
	// flux rétabli : l'enregistrement retardé de ce rebalance est ignoré, le suivant compte
	fs.healthy = true
	fs.views = []kafkaclient.GroupView{
		streamed(7, t0.Add(55*time.Second), member("m1", 0, 1), member("m2", 2)),
		streamed(8, t0.Add(65*time.Second), member("m2", 0, 1, 2)),
	}
	col.Cycle(context.Background(), t0.Add(70*time.Second))
	ev := ms.lastByStream("events")
	if len(ev) != 2 {
		t.Fatalf("2 rebalances attendus (describe puis flux), %d reçus", len(ev))
	}
	if r := rebalanceOf(ev[0]); r["method"] != MethodDescribe || ids(r["joined"])[0] != "m2" {
		t.Fatalf("repli describe attendu : %+v", r)
	}
	if r := rebalanceOf(ev[1]); r["method"] != MethodStream || ids(r["left"])[0] != "m1" || groupOf(ev[1])["generation"] != int32(8) {
		t.Fatalf("génération 8 attendue : %+v", r)
	}
}

func TestCoverageUnassigned(t *testing.T) {
	col, fb, ms := newTest(t)
	for p := int32(0); p < 3; p++ {
		fb.commits.Set("g", "t", p, kafkaclient.Commit{Offset: 100})
	}
	// l'assignateur omet la partition 2
	fb.members = described("Stable", member("m1", 0), member("m2", 1))
	check := func(at time.Duration, state string, unassigned any) {
		t.Helper()
		col.Cycle(context.Background(), t0.Add(at))
		docs := ms.lastByStream("lag")
		lag := lagOf(t, docs[len(docs)-1])
		if lag["state"] != state || lag["partitions_unassigned"] != unassigned {
			t.Fatalf("à %s : état %s et %v partition(s) non assignée(s) attendus : %+v", at, state, unassigned, lag)
		}
	}
	check(0, StateCaughtUp, 1)
	check(50*time.Second, StateCaughtUp, 1)
	check(60*time.Second, StateUnassigned, 1)
	lag := lagOf(t, ms.lastByStream("lag")[2])
	if lag["partitions_unassigned_ids"].([]int32)[0] != 2 || lag["unassigned_age_max_s"] != 60.0 {
		t.Fatalf("partition 2 non assignée depuis 60 s attendue : %+v", lag)
	}
	// pendant un rebalance la couverture n'est pas calculée et l'horloge repart
	fb.groups["g"] = kafkaclient.Group{State: "CompletingRebalance"}
	check(70*time.Second, StateCaughtUp, nil)
	fb.groups["g"] = kafkaclient.Group{State: "Stable"}
	check(80*time.Second, StateCaughtUp, 1)
}

func TestSubscriptionPairWithoutCommit(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.members = described("Stable", member("m1", 0, 1, 2))
	col.Cycle(context.Background(), t0)
	docs := ms.lastByStream("lag")
	if len(docs) != 1 {
		t.Fatalf("une paire par abonnement attendue, %d document(s)", len(docs))
	}
	if lag := lagOf(t, docs[0]); lag["state"] != StateNoCommit || lag["partitions_no_commit"] != 3 || lag["partitions_unassigned"] != 0 {
		t.Fatalf("paire sans commit attendue : %+v", lag)
	}
}

func TestManualAssignment(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.groups["g"] = kafkaclient.Group{State: "Empty"}
	fb.members = map[string]kafkaclient.GroupView{"g": {Group: "g", Generation: -1, State: "Empty"}}
	assignment := func() any {
		docs := ms.lastByStream("lag")
		return groupOf(docs[len(docs)-1])["assignment"]
	}
	for i := 0; i < 3; i++ {
		fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: int64(50 + 10*i)})
		col.Cycle(context.Background(), t0.Add(time.Duration(i)*10*time.Second))
	}
	if a := assignment(); a != AssignmentManual {
		t.Fatalf("groupe sans membre dont les commits avancent : manual attendu, reçu %v", a)
	}
	if lag := lagOf(t, ms.lastByStream("lag")[2]); lag["partitions_unassigned"] != nil || lag["state"] == StateUnassigned {
		t.Fatalf("aucune couverture pour un groupe sans membre : %+v", lag)
	}
	col.Cycle(context.Background(), t0.Add(90*time.Second))
	if a := assignment(); a != AssignmentNone {
		t.Fatalf("commits figés depuis 60 s : none attendu, reçu %v", a)
	}
}

func TestMembersLeaveIsNotManual(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.members = described("Stable", member("m1", 0, 1, 2))
	for i := 0; i < 2; i++ {
		fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: int64(50 + 10*i)})
		col.Cycle(context.Background(), t0.Add(time.Duration(i)*10*time.Second))
	}
	// le dernier membre part : son dernier commit n'est vu qu'au cycle suivant ;
	// classement absent deux cycles, puis none (jamais manual)
	fb.groups["g"] = kafkaclient.Group{State: "Empty"}
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 75})
	for _, step := range []struct {
		at   time.Duration
		want any
	}{{20 * time.Second, nil}, {30 * time.Second, nil}, {40 * time.Second, AssignmentNone}} {
		col.Cycle(context.Background(), t0.Add(step.at))
		docs := ms.lastByStream("lag")
		if cg := groupOf(docs[len(docs)-1]); cg["assignment"] != step.want || cg["members"] != 0 {
			t.Fatalf("à %s : classement %v attendu, sans membre : %+v", step.at, step.want, cg)
		}
	}
}

func TestNewGroupWaitsForStream(t *testing.T) {
	col, fs, ms := newStreamTest(t)
	fs.groups["h"] = kafkaclient.Group{State: "Stable"}
	fs.members = map[string]kafkaclient.GroupView{} // référence du premier cycle : aucun groupe décrit
	col.Cycle(context.Background(), t0)
	// cycle métriques : g et h apparaissent, l'enregistrement de création de g est encore en vol
	fs.members = map[string]kafkaclient.GroupView{
		"g": described("Stable", member("m1", 0, 1, 2))["g"],
		"h": {Group: "h", Generation: -1, ProtocolType: "consumer", State: "Stable", At: t0.Add(60 * time.Second),
			Members: []kafkaclient.Member{member("n1", 0, 1, 2)}},
	}
	col.Cycle(context.Background(), t0.Add(60*time.Second))
	if col.groups["g"].view.Group != "" || col.groups["h"].view.Group != "" {
		t.Fatal("aucune référence attendue au premier cycle où le groupe apparaît")
	}
	fs.views = []kafkaclient.GroupView{streamed(1, t0.Add(59*time.Second), member("m1", 0, 1, 2))}
	col.Cycle(context.Background(), t0.Add(70*time.Second))
	ev := ms.lastByStream("events")
	if len(ev) != 1 || groupOf(ev[0])["generation"] != int32(1) || ids(rebalanceOf(ev[0])["joined"])[0] != "m1" {
		t.Fatalf("rebalance de création de g attendu : %d événement(s)", len(ev))
	}
	// h, jamais décrit par le flux : référence au cycle métriques suivant, sans rebalance
	col.Cycle(context.Background(), t0.Add(120*time.Second))
	if col.groups["h"].view.Group != "h" || len(ms.lastByStream("events")) != 1 {
		t.Fatalf("référence de h sans rebalance attendue : %+v", col.groups["h"].view)
	}
}

func TestStreamViewSurvivesFirstDescribe(t *testing.T) {
	col, fs, ms := newStreamTest(t)
	// le groupe naît juste avant le premier cycle : son enregistrement est lu avant la référence
	fs.views = []kafkaclient.GroupView{streamed(1, t0.Add(-time.Second), member("m1", 0, 1, 2))}
	fs.members = described("Stable", member("m1", 0, 1, 2))
	col.Cycle(context.Background(), t0)
	fs.views = []kafkaclient.GroupView{streamed(2, t0.Add(5*time.Second), member("m1", 0, 1), member("m2", 2))}
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	ev := ms.lastByStream("events")
	if len(ev) != 2 || rebalanceOf(ev[1])["previous_generation"] != int32(1) {
		t.Fatalf("génération 2 rattachée à la génération 1 du flux attendue : %d événement(s), %+v", len(ev), rebalanceOf(ev[len(ev)-1]))
	}
}
