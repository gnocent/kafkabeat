package collector

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/gnocent/kafkabeat/internal/config"
	"github.com/gnocent/kafkabeat/internal/history"
	"github.com/gnocent/kafkabeat/internal/kafkaclient"
	"github.com/gnocent/kafkabeat/internal/sink"
)

// TestFillES écrit dans Elasticsearch 15 min de documents simulés (6 000
// topics de 5 partitions, 8 400 paires, 20 équipes), puis continue en temps
// réel pendant KAFKABEAT_FILL_LIVE : charge du critère 9, lancée par la
// recette avec KAFKABEAT_FILL=<url Elasticsearch>. Un topic sur dix n'a pas
// de groupe, la moitié en a deux dont un en retard, un groupe lent sur
// cinquante est bloqué.
func TestFillES(t *testing.T) {
	url := os.Getenv("KAFKABEAT_FILL")
	if url == "" {
		t.Skip("KAFKABEAT_FILL non défini")
	}
	live, _ := time.ParseDuration(os.Getenv("KAFKABEAT_FILL_LIVE"))
	const topics, parts, teams = 6000, 5, 20
	cfg := config.Default()
	cfg.Cluster = os.Getenv("KAFKABEAT_FILL_CLUSTER")
	if cfg.Cluster == "" {
		cfg.Cluster = "charge"
	}
	for i := 0; i < teams; i++ {
		cfg.Output.Owners = append(cfg.Output.Owners, config.Owner{Topic: fmt.Sprintf("team%02d-*", i), Owner: fmt.Sprintf("equipe-%02d", i)})
	}
	fb := &fakeBroker{md: kafkaclient.Metadata{ClusterID: "charge", Topics: make(map[string]kafkaclient.TopicMeta, topics)},
		leo:    make(kafkaclient.Offsets, topics),
		groups: map[string]kafkaclient.Group{}, commits: kafkaclient.GroupCommits{}}
	names := make([]string, topics)
	for i := range names {
		name := fmt.Sprintf("team%02d-logs-%04d", i%teams, i)
		names[i] = name
		tm := kafkaclient.TopicMeta{Name: name, ID: fmt.Sprint(i)}
		fb.leo[name] = make(map[int32]int64, parts)
		for p := int32(0); p < parts; p++ {
			tm.Partitions = append(tm.Partitions, kafkaclient.PartitionMeta{ID: p, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}})
			fb.leo[name][p] = 1000
		}
		fb.md.Topics[name] = tm
		if i%10 != 0 {
			fb.groups["fast-"+name] = kafkaclient.Group{State: "Stable"}
		}
		if i%10 >= 1 && i%10 <= 5 {
			fb.groups["slow-"+name] = kafkaclient.Group{State: "Stable"}
		}
	}
	advance := func(step int) {
		for i, name := range names {
			for p := range fb.leo[name] {
				fb.leo[name][p] += int64(10 + (i+int(p)+step)%40)
				if _, ok := fb.groups["fast-"+name]; ok {
					fb.commits.Set("fast-"+name, name, p, kafkaclient.Commit{Offset: fb.leo[name][p]})
				}
				if _, ok := fb.groups["slow-"+name]; ok && (i%50 != 1 || step == 0) {
					fb.commits.Set("slow-"+name, name, p, kafkaclient.Commit{Offset: fb.leo[name][p] - 2000})
				}
			}
		}
	}
	logger := log.New(os.Stderr, "", 0)
	es, err := sink.NewES(sink.ESOptions{URLs: []string{url}, Prefix: "kafkabeat", BulkMaxDocs: 2000}, logger)
	if err != nil {
		t.Fatal(err)
	}
	es.WantSetup()
	col := New(cfg, fb, fb, history.New(HistoryTiers(cfg.History)), es, logger, "charge")
	ctx := context.Background()
	start := time.Now().Truncate(10 * time.Second)
	step := 0
	for at := start.Add(-15 * time.Minute); !at.After(start); at = at.Add(10 * time.Second) {
		advance(step)
		col.Cycle(ctx, at)
		step++
	}
	fmt.Println("fill complete:", es.Stats())
	for end := time.Now().Add(live); time.Now().Before(end); {
		next := time.Now().Truncate(10 * time.Second).Add(10 * time.Second)
		time.Sleep(time.Until(next))
		advance(step)
		col.Cycle(ctx, next)
		step++
	}
	st := es.Stats()
	t.Logf("documents envoyés : %v", st)
	if st["failed"].(int64) != 0 {
		t.Fatalf("échecs d'indexation : %v", st)
	}
}
