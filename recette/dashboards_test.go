//go:build bench

package recette

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	es9URL     = env("KB_ES9", "http://localhost:9201")
	kibanaURL  = env("KB_KIBANA", "http://localhost:5601")
	kibana9URL = env("KB_KIBANA9", "http://localhost:5602")
	examples   = filepath.Join("..", "kibana", "kafkabeat-examples.ndjson")
)

// panel est une visualisation ES|QL du ndjson livré : sa requête et ses colonnes.
type panel struct {
	query   string
	columns []string
}

// savedObjects lit le ndjson livré : visualisations ES|QL par identifiant et
// nombre de panneaux par dashboard.
func savedObjects(t *testing.T) (map[string]panel, map[string]int) {
	t.Helper()
	data, err := os.ReadFile(examples)
	if err != nil {
		t.Fatal(err)
	}
	panels, dashboards := map[string]panel{}, map[string]int{}
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var obj struct {
			Type       string `json:"type"`
			ID         string `json:"id"`
			Attributes struct {
				PanelsJSON string `json:"panelsJSON"`
				State      struct {
					Query struct {
						ESQL string `json:"esql"`
					} `json:"query"`
					Datasources struct {
						TextBased struct {
							Layers map[string]struct {
								Columns []struct {
									FieldName string `json:"fieldName"`
								} `json:"columns"`
							} `json:"layers"`
						} `json:"textBased"`
					} `json:"datasourceStates"`
				} `json:"state"`
			} `json:"attributes"`
		}
		if err := json.Unmarshal(line, &obj); err != nil {
			t.Fatalf("%s: %v", examples, err)
		}
		switch {
		case obj.Type == "lens" && obj.Attributes.State.Query.ESQL != "":
			p := panel{query: obj.Attributes.State.Query.ESQL}
			for _, l := range obj.Attributes.State.Datasources.TextBased.Layers {
				for _, c := range l.Columns {
					p.columns = append(p.columns, c.FieldName)
				}
			}
			panels[obj.ID] = p
		case obj.Type == "dashboard":
			var grid []any
			json.Unmarshal([]byte(obj.Attributes.PanelsJSON), &grid)
			dashboards[obj.ID] = len(grid)
		}
	}
	return panels, dashboards
}

// esql exécute une requête comme un panneau du dashboard : filtres des
// contrôles en DSL, plage de temps en paramètres ?_tstart et ?_tend.
func esql(t *testing.T, url, query string, terms map[string]any, from, to time.Time) []map[string]any {
	t.Helper()
	var filter []map[string]any
	for k, v := range terms {
		filter = append(filter, map[string]any{"term": map[string]any{k: v}})
	}
	body := map[string]any{"query": query, "filter": map[string]any{"bool": map[string]any{"filter": filter}}}
	if strings.Contains(query, "?_tstart") {
		body["params"] = []map[string]any{{"_tstart": from.UTC().Format(time.RFC3339)}, {"_tend": to.UTC().Format(time.RFC3339)}}
	}
	raw, _ := json.Marshal(body)
	resp, err := http.Post(url+"/_query", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("ES|QL: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("ES|QL: HTTP %d: %s\n%s", resp.StatusCode, data, query)
	}
	var r struct {
		Columns []struct {
			Name string `json:"name"`
		} `json:"columns"`
		Values [][]any `json:"values"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 0, len(r.Values))
	for _, v := range r.Values {
		row := map[string]any{}
		for i, c := range r.Columns {
			row[c.Name] = v[i]
		}
		rows = append(rows, row)
	}
	return rows
}

// tiles renvoie la valeur de chaque tuile de la vue d'ensemble (0 si vide).
func tiles(t *testing.T, terms map[string]any) map[string]float64 {
	t.Helper()
	panels, _ := savedObjects(t)
	out := map[string]float64{}
	results := map[string][]map[string]any{}
	now := time.Now()
	for id, p := range panels {
		if !strings.HasPrefix(id, "kafkabeat-tile-") {
			continue
		}
		rows, ok := results[p.query]
		if !ok {
			rows = esql(t, esURL, p.query, terms, now.Add(-15*time.Minute), now)
			results[p.query] = rows
		}
		v := 0.0
		if len(rows) == 1 {
			if f, ok := rows[0][p.columns[0]].(float64); ok {
				v = f
			}
		}
		out[p.columns[0]] = v
	}
	return out
}

func checkTiles(t *testing.T, label string, got map[string]float64, want map[string]float64) {
	t.Helper()
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: tile %q = %v, expected %v", label, k, got[k], w)
		}
	}
}

func near(got, want, tolerance float64) bool {
	return math.Abs(got-want) <= tolerance*math.Abs(want)
}

// Critère 7 : sur un scénario du harnais (paire bloquée, production arrêtée,
// topic sans groupe, topic lu par deux groupes), chaque tuile de la vue
// d'ensemble vaut le compte attendu et le débit total la somme par topic ;
// les requêtes sont celles du ndjson livré, filtrées par cluster comme le
// ferait le contrôle du dashboard. Des documents synthétiques couvrent le
// seuil de 300 s, la borne haute inconnue, le rebalance et la paire disparue.
func TestC7Tiles(t *testing.T) {
	b := newBench(t, kafka39)
	topics := map[string]string{}
	for _, s := range []string{"ok", "stuck", "stopped", "orphan", "two"} {
		topics[s] = b.topic(s, 2)
	}
	// consumers d'abord : le temps qu'ils rejoignent leur groupe, les topics sont connus de tous les brokers
	flags := []string{"--commit", "auto", "--commit-interval", "1s"}
	b.consumer(b.name+"-gok", topics["ok"], flags...)
	stuck := b.consumer(b.name+"-gstuck", topics["stuck"], flags...)
	b.consumer(b.name+"-gstopped", topics["stopped"], flags...)
	b.consumer(b.name+"-gtwo-a", topics["two"], flags...)
	b.consumer(b.name+"-gtwo-b", topics["two"], flags...)
	b.producer(topics["ok"], "steady:20:10m", true)
	b.producer(topics["stuck"], "steady:20:10m", true)
	b.producer(topics["stopped"], "batch:50:10s", false)
	b.producer(topics["orphan"], "steady:10:10m", true)
	b.producer(topics["two"], "steady:50:10m", true)
	a := b.agent(agentOpts{owners: [][2]string{{topics["two"], "two"}, {b.name + "-*", "recette"}}})
	b.waitDocs(a.cluster, topics["stuck"], stuck.group, time.Time{}, 3)
	stuck.must(t, "POST", "/pause")
	// production arrêtée : plus de 60 s sans message (seuil des tuiles) après les 10 s du lot
	time.Sleep(80 * time.Second)

	cluster := map[string]any{"kafka.cluster.name": a.cluster}
	got := tiles(t, cluster)
	checkTiles(t, "scenario", got, map[string]float64{
		"Tracked topics": 5, "Consumed topics": 4, "Topics without group": 1, "Groups rebalancing": 0,
		"Consumption OK": 3, "Consumption not OK": 1, "Production active": 3, "Production stopped": 1,
	})
	// référence : documents bruts de la même fenêtre, débit produit compté une fois par topic
	docs := search(t, "kafkabeat-lag", cluster, time.Now().Add(-30*time.Second), 1000)
	produce, consume := map[string][]float64{}, map[string][]float64{}
	for _, d := range docs {
		produce[d.str("kafka.topic.name")] = append(produce[d.str("kafka.topic.name")], d.num("kafka.lag.produce_rate"))
		pair := d.str("kafka.topic.name") + "/" + d.str("kafka.consumergroup.id")
		consume[pair] = append(consume[pair], d.num("kafka.lag.consume_rate"))
	}
	refProduce, refConsume := sumOfMeans(produce), sumOfMeans(consume)
	t.Logf("rates: produced %.1f (reference %.1f, nominal 90), consumed %.1f (reference %.1f, nominal 120)",
		got["Produce rate (msg/s)"], refProduce, got["Consume rate (msg/s)"], refConsume)
	if !near(got["Produce rate (msg/s)"], refProduce, 0.05) || !near(got["Consume rate (msg/s)"], refConsume, 0.05) {
		t.Errorf("tile rates more than 5 %% away from the per-topic and per-pair sums")
	}
	// le topic lu par deux groupes ne compte qu'une fois : 90 msg/s nominal, 140 en cas de double compte
	if !near(got["Produce rate (msg/s)"], 90, 0.2) {
		t.Errorf("produce rate %.1f msg/s, expected 90 within 20 %%", got["Produce rate (msg/s)"])
	}
	if got["Last cycle (s)"] > 10 {
		t.Errorf("last cycle %v s ago", got["Last cycle (s)"])
	}
	// filtre « Équipe » (contrôle du dashboard) : seul le topic à deux groupes appartient à « deux »
	owner := tiles(t, map[string]any{"kafka.cluster.name": a.cluster, "kafka.topic.owner": "two"})
	checkTiles(t, "team two", owner, map[string]float64{
		"Tracked topics": 1, "Consumed topics": 1, "Topics without group": 0, "Consumption OK": 1, "Production active": 1,
	})
	if !near(owner["Produce rate (msg/s)"], 50, 0.2) {
		t.Errorf("team two: produce rate %.1f msg/s, expected 50", owner["Produce rate (msg/s)"])
	}

	synth := b.name + "-synth"
	indexLagDocs(t, synth, []map[string]any{
		lagDoc("seuil", "lagging", "Stable", 400.0, 390),
		lagDoc("ouverte-ko", "lagging", "Stable", nil, 500),
		lagDoc("ouverte-ok", "lagging", "Stable", nil, 100),
		lagDoc("rebalance", "caught_up", "PreparingRebalance", 0.0, 0),
		lagDoc("disparue", "gone", "Empty", nil, 0),
	})
	checkTiles(t, "synthetic documents", tiles(t, map[string]any{"kafka.cluster.name": synth}), map[string]float64{
		"Tracked topics": 5, "Consumed topics": 4, "Topics without group": 1, "Groups rebalancing": 1,
		"Consumption OK": 2, "Consumption not OK": 2, "Production active": 4, "Production stopped": 0, "Worst lag (s)": 500,
	})
}

func sumOfMeans(series map[string][]float64) float64 {
	total := 0.0
	for _, vs := range series {
		s := 0.0
		for _, v := range vs {
			s += v
		}
		total += s / float64(len(vs))
	}
	return total
}

// lagDoc construit un document de paire minimal ; upper nil = borne haute inconnue.
func lagDoc(topic, state, groupState string, upper any, lower float64) map[string]any {
	lag := map[string]any{"state": state, "seconds_lower": lower, "last_produce_age_s": 1.0, "produce_rate": 1.0, "consume_rate": 1.0}
	if upper != nil {
		lag["seconds_upper"] = upper
	}
	return map[string]any{
		"event": map[string]any{"dataset": "kafkabeat.lag", "module": "kafkabeat"},
		"kafka": map[string]any{
			"topic":         map[string]any{"name": topic},
			"consumergroup": map[string]any{"id": "g-" + topic, "state": groupState},
			"lag":           lag,
		},
	}
}

// indexLagDocs écrit les documents dans kafkabeat-lag, datés de maintenant.
func indexLagDocs(t *testing.T, cluster string, docs []map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	for _, d := range docs {
		d["@timestamp"] = ts
		d["kafka"].(map[string]any)["cluster"] = map[string]any{"name": cluster}
		buf.WriteString(`{"create":{}}` + "\n")
		line, _ := json.Marshal(d)
		buf.Write(line)
		buf.WriteString("\n")
	}
	resp, err := http.Post(esURL+"/kafkabeat-lag/_bulk?refresh=true", "application/x-ndjson", &buf)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 || bytes.Contains(data, []byte(`"errors":true`)) {
		t.Fatalf("synthetic documents: HTTP %d: %s", resp.StatusCode, data)
	}
}

// importExamples importe le ndjson livré dans un Kibana, comme le README l'indique.
func importExamples(t *testing.T, kibana string) {
	t.Helper()
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, _ := w.CreateFormFile("file", "kafkabeat-examples.ndjson")
	data, err := os.ReadFile(examples)
	if err != nil {
		t.Fatal(err)
	}
	part.Write(data)
	w.Close()
	req, _ := http.NewRequest(http.MethodPost, kibana+"/api/saved_objects/_import?overwrite=true", &body)
	req.Header.Set("kbn-xsrf", "true")
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("import %s: %v", kibana, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	var r struct {
		Success bool `json:"success"`
	}
	if json.Unmarshal(out, &r); resp.StatusCode >= 300 || !r.Success {
		t.Fatalf("import %s: HTTP %d: %s", kibana, resp.StatusCode, out)
	}
}

type rendering struct {
	LoadMs    int `json:"load_ms"`
	RefreshMs int `json:"refresh_ms"`
	Panels    int `json:"panels"`
	Errors    []struct {
		Title   string `json:"title"`
		Message string `json:"message"`
	} `json:"errors"`
	Error string `json:"error"`
}

// render ouvre le dashboard dans Chromium headless (bench/render) sur les 15
// dernières minutes ; la capture est gardée dans le répertoire du test.
func render(t *testing.T, kibana, dashboard, shot string) rendering {
	t.Helper()
	return renderRange(t, kibana, dashboard, "now-15m", "now", shot, "")
}

// renderRange : render sur une plage et avec une requête KQL (vide : aucune).
func renderRange(t *testing.T, kibana, dashboard, from, to, shot, kql string) rendering {
	t.Helper()
	args := []string{filepath.Join("..", "bench", "render", "render.mjs"), kibana, dashboard, from, to, shot}
	if kql != "" {
		args = append(args, kql)
	}
	cmd := exec.Command("node", args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, os.Stderr
	cmd.Run() // code 1 si un panneau est en erreur : le détail est dans la sortie
	var r rendering
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &r); err != nil {
		t.Fatalf("rendering %s: unreadable output: %q", dashboard, out.String())
	}
	if r.Error != "" {
		t.Fatalf("rendering %s on %s: %s", dashboard, kibana, r.Error)
	}
	return r
}

// Critère 8 : le ndjson livré (exporté de 8.19) s'importe et ses dashboards
// s'affichent sans erreur de panneau sur Kibana 8.19 et 9.5, chacun alimenté
// par un agent.
func TestC8DashboardsImportAndRender(t *testing.T) {
	b := newBench(t, kafka39)
	topic := b.topic("t", 2)
	b.producer(topic, "steady:20:10m", true)
	cons := b.consumer(b.name+"-g", topic, "--commit", "auto", "--commit-interval", "1s")
	owners := [][2]string{{b.name + "-*", "recette"}}
	a8 := b.agent(agentOpts{owners: owners})
	b.agent(agentOpts{es: es9URL, owners: owners})
	b.waitDocs(a8.cluster, topic, cons.group, time.Time{}, 5)
	// au moins un cycle métriques (20 s en recette) dans les deux Elasticsearch
	time.Sleep(30 * time.Second)
	_, dashboards := savedObjects(t)
	for _, k := range []struct{ version, url string }{{"8.19", kibanaURL}, {"9.5", kibana9URL}} {
		importExamples(t, k.url)
		for id, n := range dashboards {
			r := render(t, k.url, id, filepath.Join(b.dir, fmt.Sprintf("%s-%s.png", k.version, id)))
			t.Logf("Kibana %s, %s: %d panels, load %d ms, refresh %d ms", k.version, id, r.Panels, r.LoadMs, r.RefreshMs)
			if r.Panels != n {
				t.Errorf("Kibana %s, %s: %d panels rendered, %d expected", k.version, id, r.Panels, n)
			}
			for _, e := range r.Errors {
				t.Errorf("Kibana %s, %s, panel %q: %s", k.version, id, e.Title, e.Message)
			}
		}
	}
}

// Critère 9 : 15 min simulées (30 000 partitions, 6 000 topics, 8 400 paires)
// écrites dans Elasticsearch, la collecte simulée continuant en temps réel ;
// la vue d'ensemble se rafraîchit en moins de 5 s.
func TestC9OverviewUnderLoad(t *testing.T) {
	cmd := exec.Command(goBin, "test", "-count=1", "-timeout", "40m", "-run", "TestFillES", "-v", "./internal/collector/")
	cmd.Dir = ".."
	cmd.Env = append(os.Environ(), "KAFKABEAT_FILL="+esURL, "KAFKABEAT_FILL_LIVE=90s", "KAFKABEAT_FILL_CLUSTER="+runID+"-charge")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// arrêt de la simulation si le test échoue avant sa fin
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
		}
	})
	ready := make(chan struct{})
	var readyOnce sync.Once
	var logMu sync.Mutex
	var log bytes.Buffer
	logText := func() string {
		logMu.Lock()
		defer logMu.Unlock()
		return log.String()
	}
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			logMu.Lock()
			log.WriteString(sc.Text() + "\n")
			logMu.Unlock()
			if strings.Contains(sc.Text(), "fill complete") {
				readyOnce.Do(func() { close(ready) })
			}
		}
	}()
	select {
	case <-ready:
	case <-time.After(30 * time.Minute):
		cmd.Process.Kill()
		t.Fatalf("fill took too long:\n%s", logText())
	}
	importExamples(t, kibanaURL)
	r := render(t, kibanaURL, "kafkabeat-dashboard", filepath.Join(t.TempDir(), "charge.png"))
	t.Logf("overview under load: load %d ms, refresh %d ms", r.LoadMs, r.RefreshMs)
	for _, e := range r.Errors {
		t.Errorf("panel %q: %s", e.Title, e.Message)
	}
	if r.RefreshMs >= 5000 {
		t.Errorf("overview refreshed in %d ms, expected < 5 s", r.RefreshMs)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("simulation: %v\n%s", err, logText())
	}
}
