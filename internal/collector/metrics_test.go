package collector

import (
	"context"
	"testing"
	"time"

	"github.com/gnocent/kafkabeat/internal/kafkaclient"
)

func TestMetricsDocs(t *testing.T) {
	col, fb, ms := newTest(t)
	// broker 2 annoncé en IPv6 : adresse encadrée dans les documents
	fb.md.Brokers = []kafkaclient.BrokerMeta{{ID: 1, Host: "b1", Port: 9092}, {ID: 2, Host: "fd00::2", Port: 9092}}
	tm := fb.md.Topics["t"]
	for i := range tm.Partitions {
		tm.Partitions[i].Leader = 1
		tm.Partitions[i].Replicas = []int32{1, 2}
		tm.Partitions[i].ISR = []int32{1}
	}
	fb.md.Topics["t"] = tm
	fb.start = kafkaclient.Offsets{"t": {0: 10, 1: 10, 2: 10}}
	fb.groups["g"] = kafkaclient.Group{State: "Empty", Coordinator: 2}
	fb.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 100, Metadata: "m0"})
	fb.commits.Set("g", "t", 1, kafkaclient.Commit{Offset: 90})
	fb.members = map[string]kafkaclient.GroupView{"g": {Group: "g", Generation: -1, State: "Empty", ProtocolType: "consumer",
		Members: []kafkaclient.Member{{ID: "mid", ClientID: "app", ClientHost: "/10.0.0.1", Assigned: map[string][]int32{"t": {0}}}}}}

	// premier cycle : cycle métriques (3 partitions x 2 réplicas, 2 partitions commitées)
	col.Cycle(context.Background(), t0)
	metrics := ms.lastByStream("metrics")
	var parts, groups int
	for _, d := range metrics {
		switch d.Body["event"].(map[string]any)["dataset"] {
		case "kafka.partition":
			parts++
			k := d.Body["kafka"].(map[string]any)
			p := k["partition"].(map[string]any)
			pp := p["partition"].(map[string]any)
			if p["offset"].(map[string]any)["oldest"].(int64) != 10 || p["offset"].(map[string]any)["newest"].(int64) != 100 {
				t.Fatalf("offsets inattendus : %+v", p)
			}
			replica := pp["replica"].(int32)
			if pp["is_leader"].(bool) != (replica == 1) || pp["insync_replica"].(bool) != (replica == 1) {
				t.Fatalf("réplica %d : leader/ISR incohérents : %+v", replica, pp)
			}
			if k["broker"].(map[string]any)["address"] != map[int32]string{1: "b1:9092", 2: "[fd00::2]:9092"}[replica] {
				t.Fatalf("adresse du broker inattendue : %+v", k["broker"])
			}
			if p["topic_id"] != "0-t" && p["topic_id"] != "1-t" && p["topic_id"] != "2-t" {
				t.Fatalf("topic_id inattendu : %v", p["topic_id"])
			}
		case "kafka.consumergroup":
			groups++
			k := d.Body["kafka"].(map[string]any)
			cg := k["consumergroup"].(map[string]any)
			if cg["id"] != "g" || k["broker"].(map[string]any)["address"] != "[fd00::2]:9092" {
				t.Fatalf("document consumergroup inattendu : %+v", k)
			}
			if k["partition"].(map[string]any)["id"].(int32) == 1 {
				if cg["consumer_lag"].(int64) != 10 || k["lag"].(map[string]any)["method"] != "bracket_open" {
					t.Fatalf("partition 1 : lag 10 et encadrement ouvert attendus : %+v", k)
				}
				if _, ok := cg["client"]; ok || cg["meta"] != "" {
					t.Fatalf("partition 1 : ni membre assigné ni métadonnée attendus : %+v", cg)
				}
			} else {
				if cg["consumer_lag"].(int64) != 0 || k["lag"].(map[string]any)["seconds_upper"] != 0.0 {
					t.Fatalf("partition 0 : à jour attendu : %+v", k)
				}
				client, _ := cg["client"].(map[string]any)
				if cg["meta"] != "m0" || client["id"] != "app" || client["host"] != "/10.0.0.1" || client["member_id"] != "mid" {
					t.Fatalf("partition 0 : meta et client attendus comme Metricbeat : %+v", cg)
				}
			}
		}
	}
	if parts != 6 || groups != 2 {
		t.Fatalf("6 documents partition et 2 consumergroup attendus, reçus %d et %d", parts, groups)
	}
	// cycle suivant, avant l'échéance métriques : aucun document métriques
	col.Cycle(context.Background(), t0.Add(10*time.Second))
	if n := len(ms.lastByStream("metrics")); n != 8 {
		t.Fatalf("aucun document métriques attendu entre deux échéances, total %d", n)
	}
	col.Cycle(context.Background(), t0.Add(60*time.Second))
	if n := len(ms.lastByStream("metrics")); n != 16 {
		t.Fatalf("nouveaux documents métriques attendus à l'échéance, total %d", n)
	}
}
