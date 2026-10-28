package collector

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gnocent/kafkabeat/internal/kafkaclient"
)

func TestRetentionRisk(t *testing.T) {
	col, fb, ms := newTest(t)
	fb.groups["g"] = kafkaclient.Group{State: "Stable", Type: "consumer"}
	// p0 : log start à 0, risque inconnu ; p1 : 40 des 50 offsets retenus non lus ;
	// p2 : commit 70 sous le log start 80, 10 offsets perdus
	fb.start = kafkaclient.Offsets{"t": {0: 0, 1: 50, 2: 80}}
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 40})
	fb.commits.Set("g", "t", 1, kafkaclient.Commit{Offset: 60})
	fb.commits.Set("g", "t", 2, kafkaclient.Commit{Offset: 70})
	col.Cycle(context.Background(), t0)
	doc := ms.lastByStream("lag")[0]
	lag := lagOf(t, doc)
	if lag["retention_used"].(float64) != 1 || lag["offsets_lost"].(int64) != 10 || !lag["flags"].(map[string]bool)[flagLogStartPassed] {
		t.Fatalf("perte attendue sur la paire : %+v", lag)
	}
	if typ := doc.Body["kafka"].(map[string]any)["consumergroup"].(map[string]any)["type"]; typ != "consumer" {
		t.Fatalf("type de groupe attendu consumer : %v", typ)
	}
	byPart := map[int32]map[string]any{}
	for _, d := range ms.lastByStream("metrics") {
		if d.Body["event"].(map[string]any)["dataset"] == "kafka.consumergroup" {
			k := d.Body["kafka"].(map[string]any)
			byPart[k["partition"].(map[string]any)["id"].(int32)] = k["lag"].(map[string]any)
		}
	}
	if _, ok := byPart[0]["retention_used"]; ok {
		t.Fatalf("p0 (log start 0) : aucun risque attendu : %+v", byPart[0])
	}
	if byPart[1]["retention_used"].(float64) != 0.8 || byPart[1]["commit_before_log_start"] != false {
		t.Fatalf("p1 : part 0,8 sans perte attendue : %+v", byPart[1])
	}
	if byPart[2]["retention_used"].(float64) != 1 || byPart[2]["commit_before_log_start"] != true || byPart[2]["offsets_lost"].(int64) != 10 {
		t.Fatalf("p2 : perte de 10 offsets attendue : %+v", byPart[2])
	}

	// cycle suivant, sans relecture du log start : p2 repasse devant, le flag
	// reste levé, la part se calcule sur le log start gardé
	fb.commits.Set("g", "t", 2, kafkaclient.Commit{Offset: 85})
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	lag = lagOf(t, ms.lastByStream("lag")[1])
	if lag["retention_used"].(float64) != 0.8 || lag["offsets_lost"] != nil || !lag["flags"].(map[string]bool)[flagLogStartPassed] {
		t.Fatalf("part 0,8, perte passée mais flag tenu attendus : %+v", lag)
	}

	// topic recréé : le log start gardé ne décrit plus le journal
	fb.md.Topics["t"] = kafkaclient.TopicMeta{Name: "t", ID: "new", Partitions: fb.md.Topics["t"].Partitions}
	col.Cycle(context.Background(), t0.Add(20*time.Second))
	if lag = lagOf(t, ms.lastByStream("lag")[2]); lag["retention_used"] != nil {
		t.Fatalf("aucun risque attendu après recréation, avant relecture du log start : %+v", lag)
	}
}

func TestHealthCycleFailed(t *testing.T) {
	col, fb, ms := newTest(t)
	col.Cycle(context.Background(), t0)
	fb.mdErr = errors.New("injoignable")
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	h := ms.lastByStream("health")
	if len(h) != 2 {
		t.Fatalf("2 documents de santé attendus, %d reçus", len(h))
	}
	for i, want := range []bool{false, true} {
		if got := h[i].Body["kafkabeat"].(map[string]any)["cycle_failed"]; got != want {
			t.Fatalf("cycle %d : cycle_failed %v, attendu %v", i+1, got, want)
		}
	}
}
