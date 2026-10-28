package collector

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/gnocent/kafkabeat/internal/config"
	"github.com/gnocent/kafkabeat/internal/history"
	"github.com/gnocent/kafkabeat/internal/kafkaclient"
	"github.com/gnocent/kafkabeat/internal/sink"
)

type nullSink struct{ docs int }

func (n *nullSink) Emit(d []sink.Doc) error { n.docs += len(d); return nil }

// TestLoad30k simule 625 topics de 48 partitions (30 000 partitions) suivis
// par 2 groupes chacun, dont un en retard, et mesure la durée du cycle et la
// mémoire. Lancé seulement avec KAFKABEAT_LOAD=1 (critère 6 de DESIGN.md).
func TestLoad30k(t *testing.T) {
	if os.Getenv("KAFKABEAT_LOAD") == "" {
		t.Skip("KAFKABEAT_LOAD non défini")
	}
	const topics, parts = 625, 48
	cfg := config.Default()
	cfg.Cluster = "load"
	fb := &fakeBroker{md: kafkaclient.Metadata{Topics: make(map[string]kafkaclient.TopicMeta, topics)},
		leo: make(kafkaclient.Offsets, topics), groups: map[string]kafkaclient.Group{}, commits: kafkaclient.GroupCommits{}}
	for i := 0; i < topics; i++ {
		name := fmt.Sprintf("logs-%04d", i)
		tm := kafkaclient.TopicMeta{Name: name, ID: fmt.Sprint(i)}
		fb.leo[name] = make(map[int32]int64, parts)
		for p := int32(0); p < parts; p++ {
			tm.Partitions = append(tm.Partitions, kafkaclient.PartitionMeta{ID: p, Leader: 1})
			fb.leo[name][p] = 1000
			fb.commits.Set("fast-"+name, name, p, kafkaclient.Commit{Offset: 1000})
			fb.commits.Set("slow-"+name, name, p, kafkaclient.Commit{Offset: 500})
		}
		fb.md.Topics[name] = tm
		fb.groups["fast-"+name] = kafkaclient.Group{State: "Stable"}
		fb.groups["slow-"+name] = kafkaclient.Group{State: "Stable"}
	}
	out := &nullSink{}
	col := New(cfg, fb, fb, history.New(HistoryTiers(cfg.History)), out, log.New(os.Stderr, "", 0), "load")
	now := t0
	var total time.Duration
	const cycles = 30
	for i := 0; i < cycles; i++ {
		now = now.Add(10 * time.Second)
		for name, ps := range fb.leo {
			for p := range ps {
				ps[p] += int64(100 + (i+int(p))%50)
				fb.commits["fast-"+name][name][p] = kafkaclient.Commit{Offset: ps[p]}
				fb.commits["slow-"+name][name][p] = kafkaclient.Commit{Offset: ps[p] - 500}
			}
		}
		start := time.Now()
		col.Cycle(context.Background(), now)
		total += time.Since(start)
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	avg := total / cycles
	t.Logf("30 000 partitions, %d paires : cycle moyen %s, %d documents, heap %d Mo", topics*2, avg, out.docs, ms.HeapAlloc>>20)
	dir := t.TempDir()
	start := time.Now()
	if err := col.Save(dir, now); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(dir + "/history.snap")
	t.Logf("snapshot en %s, historique %d Mo", time.Since(start), st.Size()>>20)
	if avg > 5*time.Second {
		t.Fatalf("cycle moyen %s > 5 s", avg)
	}
	if ms.HeapAlloc > 2<<30 {
		t.Fatalf("heap %d Mo > 2 Go", ms.HeapAlloc>>20)
	}
}
