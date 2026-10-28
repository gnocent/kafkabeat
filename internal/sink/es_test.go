package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

type fakeES struct {
	mu          sync.Mutex
	puts        []string
	rollovers   []string
	bulkLines   [][]string
	rejectIdx   int // index (dans le lot) d'un document à refuser, -1 sinon
	conflictIdx int // index (dans le lot) d'un doublon TSDS (409), -1 sinon
	templates   map[string]map[string]any
}

func (f *fakeES) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.Method == http.MethodGet:
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut:
			f.puts = append(f.puts, r.URL.Path)
			if strings.HasPrefix(r.URL.Path, "/_index_template/") {
				var tpl map[string]any
				json.NewDecoder(r.Body).Decode(&tpl)
				if f.templates == nil {
					f.templates = map[string]map[string]any{}
				}
				f.templates[strings.TrimPrefix(r.URL.Path, "/_index_template/")] = tpl
			}
			w.Write([]byte(`{"acknowledged":true}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_rollover"):
			// seul kb-lag existe déjà : les autres data streams sont créés au premier envoi
			f.rollovers = append(f.rollovers, r.URL.Path)
			if r.URL.Path != "/kb-lag/_rollover" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Write([]byte(`{"acknowledged":true,"rolled_over":true}`))
		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			if r.Header.Get("Content-Type") != "application/x-ndjson" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var lines []string
			sc := bufio.NewScanner(r.Body)
			for sc.Scan() {
				lines = append(lines, sc.Text())
			}
			f.bulkLines = append(f.bulkLines, lines)
			items := make([]map[string]any, 0, len(lines)/2)
			for i := 0; i < len(lines)/2; i++ {
				switch i {
				case f.rejectIdx:
					items = append(items, map[string]any{"create": map[string]any{"status": 400, "error": map[string]any{"type": "mapper_parsing_exception", "reason": "bad"}}})
				case f.conflictIdx:
					items = append(items, map[string]any{"create": map[string]any{"status": 409, "error": map[string]any{"type": "version_conflict_engine_exception", "reason": "dup"}}})
				default:
					items = append(items, map[string]any{"create": map[string]any{"status": 201}})
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"errors": f.rejectIdx >= 0 || f.conflictIdx >= 0, "items": items})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

func TestSetupAndBulk(t *testing.T) {
	fake := &fakeES{rejectIdx: -1, conflictIdx: -1}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	es, err := NewES(ESOptions{URLs: []string{"http://127.0.0.1:1", srv.URL + "/"}, Prefix: "kb", BulkMaxDocs: 2}, log.New(os.Stderr, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := es.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"/_ilm/policy/kb": true, "/_ilm/policy/kb-timeseries": true, "/_index_template/kb-lag": true, "/_index_template/kb-metrics": true, "/_index_template/kb-health": true,
		"/_index_template/kb-events": true}
	for _, p := range fake.puts {
		delete(want, p)
	}
	if len(want) != 0 {
		t.Fatalf("installations manquantes : %v (reçues : %v)", want, fake.puts)
	}
	if len(fake.rollovers) != 4 {
		t.Fatalf("un rollover par template remis à jour attendu (absent toléré), reçus : %v", fake.rollovers)
	}
	docs := []Doc{
		{Stream: "lag", Body: map[string]any{"a": 1}},
		{Stream: "lag", Body: map[string]any{"a": 2}},
		{Stream: "health", Body: map[string]any{"b": 1}},
	}
	if err := es.Emit(docs); err != nil {
		t.Fatal(err)
	}
	if len(fake.bulkLines) != 2 || len(fake.bulkLines[0]) != 4 || len(fake.bulkLines[1]) != 2 {
		t.Fatalf("lots attendus 2+1 documents : %v", fake.bulkLines)
	}
	if !strings.Contains(fake.bulkLines[0][0], `"_index":"kb-lag"`) || !strings.Contains(fake.bulkLines[0][0], `"create"`) {
		t.Fatalf("action create vers kb-lag attendue : %s", fake.bulkLines[0][0])
	}
	if !strings.Contains(fake.bulkLines[1][0], `"_index":"kb-health"`) {
		t.Fatalf("action vers kb-health attendue : %s", fake.bulkLines[1][0])
	}
	st := es.Stats()
	if st["sent"].(int64) != 3 || st["failed"].(int64) != 0 || st["requests"].(int64) != 2 {
		t.Fatalf("compteurs inattendus : %v", st)
	}
	// un document refusé par Elasticsearch est compté et remonté
	fake.rejectIdx = 1
	if err := es.Emit(docs[:2]); err == nil || !strings.Contains(err.Error(), "mapper_parsing_exception") {
		t.Fatalf("erreur de mapping attendue, reçu %v", err)
	}
	if st := es.Stats(); st["failed"].(int64) != 1 || st["sent"].(int64) != 4 {
		t.Fatalf("compteurs après refus : %v", st)
	}
	// un doublon TSDS (409) n'est ni une erreur ni un échec, mais il est compté
	fake.rejectIdx, fake.conflictIdx = -1, 0
	if err := es.Emit(docs[:2]); err != nil {
		t.Fatalf("un 409 ne doit pas être une erreur : %v", err)
	}
	if st := es.Stats(); st["failed"].(int64) != 1 || st["sent"].(int64) != 5 || st["duplicates"].(int64) != 1 {
		t.Fatalf("compteurs après doublon : %v", st)
	}
	// lag et métriques en TSDS (politique kb-timeseries), santé et événements classiques
	for name, wantDims := range map[string][]string{"kb-lag": lagDimensions, "kb-metrics": metricsDimensions, "kb-health": nil, "kb-events": nil} {
		settings := fake.templates[name]["template"].(map[string]any)["settings"].(map[string]any)
		if (settings["index.mode"] == "time_series") != (wantDims != nil) {
			t.Fatalf("%s : index.mode %v", name, settings["index.mode"])
		}
		if wantDims == nil {
			if settings["index.lifecycle.name"] != "kb" {
				t.Fatalf("%s : politique %v", name, settings["index.lifecycle.name"])
			}
			continue
		}
		if settings["index.lifecycle.name"] != "kb-timeseries" {
			t.Fatalf("%s : politique %v", name, settings["index.lifecycle.name"])
		}
		mapping := fake.templates[name]["template"].(map[string]any)["mappings"].(map[string]any)
		for _, path := range wantDims {
			field := mapping
			for _, p := range strings.Split(path, ".") {
				if props, ok := field["properties"].(map[string]any); ok {
					field = props[p].(map[string]any)
				}
			}
			if field["time_series_dimension"] != true {
				t.Fatalf("%s : %s non marqué dimension", name, path)
			}
		}
		for _, r := range settings["index.routing_path"].([]any) {
			if r == "kafka.partition.id" || r == "kafka.broker.id" {
				t.Fatalf("%s : dimension numérique %v dans routing_path", name, r)
			}
		}
	}
}

func TestEnsureSetupOnceThenEmit(t *testing.T) {
	fake := &fakeES{rejectIdx: -1, conflictIdx: -1}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	es, err := NewES(ESOptions{URLs: []string{srv.URL}, Prefix: "kb"}, log.New(os.Stderr, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	es.WantSetup()
	if err := es.EnsureSetup(context.Background()); err != nil {
		t.Fatal(err)
	}
	n := len(fake.puts)
	if n == 0 {
		t.Fatal("installation attendue au démarrage")
	}
	if err := es.EnsureSetup(context.Background()); err != nil || len(fake.puts) != n {
		t.Fatalf("une seule installation attendue : %d puis %d (%v)", n, len(fake.puts), err)
	}
	if err := es.Emit([]Doc{{Stream: "lag", Body: map[string]any{}}}); err != nil || len(fake.puts) != n {
		t.Fatalf("l'envoi ne doit pas réinstaller : %v, %d installations", err, len(fake.puts))
	}
	// échec au démarrage (Elasticsearch injoignable) : retentée à l'envoi
	down, err := NewES(ESOptions{URLs: []string{"http://127.0.0.1:1"}, Prefix: "kb"}, log.New(os.Stderr, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	down.WantSetup()
	if err := down.EnsureSetup(context.Background()); err == nil {
		t.Fatal("erreur attendue quand aucune URL ne répond")
	}
	if err := down.Emit([]Doc{{Stream: "lag", Body: map[string]any{}}}); err == nil || !strings.Contains(err.Error(), "installing templates") {
		t.Fatalf("l'envoi doit retenter l'installation : %v", err)
	}
}

func TestAllURLsDown(t *testing.T) {
	es, err := NewES(ESOptions{URLs: []string{"http://127.0.0.1:1"}, Prefix: "kb", BulkMaxDocs: 1}, log.New(os.Stderr, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	docs := make([]Doc, 5)
	for i := range docs {
		docs[i] = Doc{Stream: "lag", Body: map[string]any{"i": i}}
	}
	if err := es.Emit(docs); err == nil || !strings.Contains(err.Error(), "4 document(s) dropped") {
		t.Fatalf("une erreur de transport doit être renvoyée et les lots restants abandonnés : %v", err)
	}
	// un seul lot essayé : les quatre autres ne doivent pas attendre un délai chacun
	if st := es.Stats(); st["requests"].(int64) != 1 || st["failed"].(int64) != 5 || st["last_error"] == "" {
		t.Fatalf("compteurs inattendus : %v", st)
	}
}

func TestFailoverOnUnavailable(t *testing.T) {
	// première URL : nœud ou mandataire indisponible (503) ; seconde : saine
	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"no master"}`, http.StatusServiceUnavailable)
	}))
	defer unavailable.Close()
	fake := &fakeES{rejectIdx: -1, conflictIdx: -1}
	healthy := httptest.NewServer(fake.handler())
	defer healthy.Close()
	es, err := NewES(ESOptions{URLs: []string{unavailable.URL, healthy.URL}, Prefix: "kb"}, log.New(os.Stderr, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := es.Emit([]Doc{{Stream: "lag", Body: map[string]any{}}}); err != nil {
		t.Fatalf("un 503 doit faire basculer sur l'URL suivante : %v", err)
	}
	if len(fake.bulkLines) != 1 {
		t.Fatalf("le lot doit être arrivé sur l'URL saine : %v", fake.bulkLines)
	}
	// l'URL saine devient la courante : plus aucun essai sur la première
	if err := es.Emit([]Doc{{Stream: "lag", Body: map[string]any{}}}); err != nil || len(fake.bulkLines) != 2 {
		t.Fatalf("second envoi : %v, %d lots", err, len(fake.bulkLines))
	}
	if st := es.Stats(); st["sent"].(int64) != 2 || st["failed"].(int64) != 0 {
		t.Fatalf("compteurs inattendus : %v", st)
	}
}

func TestMappingsAreValidJSON(t *testing.T) {
	for name, m := range map[string]map[string]any{"lag": lagMapping(), "metrics": metricsMapping(), "health": healthMapping(), "events": eventsMapping()} {
		if _, err := json.Marshal(indexTemplate("kb-"+name, "kb", m, nil)); err != nil {
			t.Fatalf("%s : %v", name, err)
		}
	}
	for _, p := range []map[string]any{ilmPolicy(), timeSeriesPolicy()} {
		if _, err := json.Marshal(p); err != nil {
			t.Fatal(err)
		}
	}
}
