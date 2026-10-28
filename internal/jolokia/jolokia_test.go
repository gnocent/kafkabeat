package jolokia

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gnocent/kafkabeat/internal/kafkaclient"
	"github.com/gnocent/kafkabeat/internal/sink"
)

type memSink struct{ docs []sink.Doc }

func (m *memSink) Emit(docs []sink.Doc) error { m.docs = append(m.docs, docs...); return nil }

// recorded sert la réponse enregistrée d'un agent du banc et garde la requête reçue.
func recorded(t *testing.T, file string, got *[]request, query *string) *httptest.Server {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if got != nil {
			json.Unmarshal(body, got)
			*query = r.URL.RawQuery
		}
		w.Write(data)
	}))
}

func get(m map[string]any, dotted string) any {
	var v any = m
	for _, k := range strings.Split(dotted, ".") {
		mm, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = mm[k]
	}
	return v
}

func TestPollBrokers(t *testing.T) {
	var sent []request
	var query string
	s39 := recorded(t, "testdata/kafka39.json", &sent, &query)
	defer s39.Close()
	s44 := recorded(t, "testdata/kafka44.json", nil, nil)
	defer s44.Close()
	// broker coupé (connexion refusée) et broker muet (ne répond jamais)
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	closed := "http://" + l.Addr().String() + "/jolokia/"
	l.Close()
	mute := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	}))
	defer mute.Close()

	urls := map[int32]string{1: s39.URL + "/jolokia/", 2: s44.URL + "/jolokia/", 3: closed, 4: mute.URL + "/jolokia/"}
	brokers := func(context.Context) (string, []kafkaclient.BrokerMeta, error) {
		return "cid", []kafkaclient.BrokerMeta{{ID: 4, Host: "b4", Port: 9092}, {ID: 1, Host: "b1", Port: 9092},
			{ID: 2, Host: "b2", Port: 9092}, {ID: 3, Host: "b3", Port: 9092}}, nil
	}
	ms := &memSink{}
	p := New(Options{Port: 8778, Path: "jolokia", Timeout: 500 * time.Millisecond, Period: time.Minute, Cluster: "c",
		Fields: map[string]string{"site": "lab"}, Owner: func(string) string { return "equipe" }}, brokers, ms, log.New(io.Discard, "", 0))
	if u := p.url(kafkaclient.BrokerMeta{Host: "b1"}); u != "http://b1:8778/jolokia/" {
		t.Fatalf("adresse Jolokia inattendue : %s", u)
	}
	if u := p.url(kafkaclient.BrokerMeta{Host: "fd00::2"}); u != "http://[fd00::2]:8778/jolokia/" {
		t.Fatalf("un broker annoncé en IPv6 doit donner une URL valide : %s", u)
	}
	p.url = func(b kafkaclient.BrokerMeta) string { return urls[b.ID] }

	start := time.Now()
	p.Poll(context.Background(), time.Date(2026, 10, 27, 13, 0, 7, 0, time.UTC))
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("un broker muet ne doit pas retenir l'interrogation au-delà du délai : %s", d)
	}

	// requête : une lecture par couple MBean + attribut distinct, noms canoniques
	if len(sent) != 18 || query != "canonicalNaming=true" {
		t.Fatalf("18 lectures et canonicalNaming=true attendus : %d, %q", len(sent), query)
	}
	// brokers 1 (Jolokia 1.x) et 2 (Jolokia 2.x) émis : un document fixe chacun et un par MBean de topic
	byBroker := map[int32]map[string]map[string]any{} // broker -> mbean ("" : fixe) -> kafka
	for _, doc := range ms.docs {
		k := doc.Body["kafka"].(map[string]any)
		id := k["broker"].(map[string]any)["id"].(int32)
		if byBroker[id] == nil {
			byBroker[id] = map[string]map[string]any{}
		}
		mbean, _ := get(k, "broker.mbean").(string)
		byBroker[id][mbean] = k
	}
	if len(byBroker) != 2 || byBroker[1] == nil || byBroker[2] == nil {
		t.Fatalf("documents des brokers 1 et 2 seulement attendus : %v", byBroker)
	}
	d := ms.docs[0].Body
	if d["@timestamp"] != "2026-10-27T13:00:00Z" || d["site"] != "lab" || ms.docs[0].Stream != "metrics" ||
		get(d, "event.dataset") != "kafka.broker" || get(d, "metricset.name") != "broker" || get(d, "metricset.period") != int64(60000) ||
		get(d, "service.address") != "b1:8778" || get(d, "kafka.broker.address") != "b1:9092" || get(d, "kafka.cluster.id") != "cid" {
		t.Fatalf("en-tête inattendu : %+v", d)
	}
	for id, docs := range byBroker {
		b := docs[""]["broker"].(map[string]any)
		for _, f := range []string{"request.channel.queue.size", "request.produce.failed", "request.fetch.failed",
			"replication.unclean_leader_elections", "log.flush_rate", "messages_in", "net.in.bytes_per_sec", "net.out.bytes_per_sec", "net.rejected.bytes_per_sec"} {
			if _, ok := get(b, f).(float64); !ok {
				t.Fatalf("broker %d : champ %s attendu : %+v", id, f, b)
			}
		}
		// dernier champ cité du manifeste, MBeans absents en KRaft ignorés (404 en 1.x comme en 2.x)
		for _, f := range []string{"request.produce.failed_per_second", "replication.leader_elections", "session", "mbean"} {
			if get(b, f) != nil {
				t.Fatalf("broker %d : champ %s inattendu : %+v", id, f, b)
			}
		}
		tp := docs["kafka.server:name=BytesInPerSec,topic=jolokia-probe,type=BrokerTopicMetrics"]
		if get(tp, "broker.topic.net.in.bytes_per_sec") == nil || get(tp, "topic.name") != "jolokia-probe" || get(tp, "topic.owner") != "equipe" {
			t.Fatalf("broker %d : document par topic inattendu : %+v", id, tp)
		}
	}

	h, errs := p.Health()
	if h["brokers"] != 4 || h["brokers_failed"] != 2 || h["docs"] != len(ms.docs) || len(errs) != 2 ||
		!strings.Contains(errs[0], "broker 3") || !strings.Contains(errs[1], "broker 4") {
		t.Fatalf("santé : 2 brokers en échec attendus : %v %v", h, errs)
	}
}

func TestPollMetadataError(t *testing.T) {
	brokers := func(context.Context) (string, []kafkaclient.BrokerMeta, error) {
		return "", nil, context.DeadlineExceeded
	}
	ms := &memSink{}
	p := New(Options{Port: 8778, Timeout: time.Second, Period: time.Minute}, brokers, ms, log.New(io.Discard, "", 0))
	if h, _ := p.Health(); h != nil {
		t.Fatal("aucune section avant la première interrogation")
	}
	p.Poll(context.Background(), time.Now())
	if _, errs := p.Health(); len(errs) != 1 || len(ms.docs) != 0 {
		t.Fatalf("erreur de métadonnées attendue : %v", errs)
	}
}
