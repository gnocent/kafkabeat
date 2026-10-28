//go:build bench

package recette

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// esDo envoie une requête JSON à Elasticsearch ou à Kibana et décode la réponse.
func esDo(t *testing.T, method, url string, body any) (int, map[string]any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, url, rd)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("kbn-xsrf", "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var out map[string]any
	json.Unmarshal(data, &out)
	if out == nil {
		out = map[string]any{"_raw": string(data)}
	}
	return resp.StatusCode, out
}

func esMust(t *testing.T, method, url string, body any) map[string]any {
	t.Helper()
	status, out := esDo(t, method, url, body)
	if status >= 300 {
		t.Fatalf("%s %s: HTTP %d: %v", method, url, status, out)
	}
	return out
}

func (b *bench) topicWith(suffix string, partitions int, config string) string {
	name := b.name + "-" + suffix
	b.harness("topics", "create", "--name", name, "--partitions", fmt.Sprint(partitions), "--config", config)
	b.topics = append(b.topics, name)
	return name
}

// Identifiants des règles d'exemple (kibana/examples.py).
const (
	ruleMute       = "8d2e4a61-0b7c-4f3e-9a15-6c8b2d0e4f71"
	ruleProduction = "5f1a9c3e-7d2b-4e80-b6a4-1e9c7f3d2a58"
)

// tuneRule réécrit les paramètres d'une règle importée, l'exécute toutes les
// 15 s et l'active, puis attend sa première exécution réussie ; elle est
// désactivée en fin de test.
func tuneRule(t *testing.T, kibana, id string, mutate func(params map[string]any)) {
	t.Helper()
	rule := esMust(t, http.MethodGet, kibana+"/api/alerting/rule/"+id, nil)
	params := rule["params"].(map[string]any)
	mutate(params)
	esMust(t, http.MethodPut, kibana+"/api/alerting/rule/"+id, map[string]any{
		"name": rule["name"], "tags": rule["tags"], "actions": []any{}, "params": params,
		"schedule": map[string]any{"interval": "15s"},
	})
	// Kibana 8.19 : une règle réimportée dont la tâche (désactivée) existe encore se dit active au premier
	// _enable sans jamais s'exécuter ; désactiver puis réactiver relance la tâche (constaté sur le banc)
	for _, action := range []string{"_enable", "_disable", "_enable"} {
		esMust(t, http.MethodPost, kibana+"/api/alerting/rule/"+id+"/"+action, nil)
	}
	enabledAt := time.Now()
	t.Cleanup(func() { esDo(t, http.MethodPost, kibana+"/api/alerting/rule/"+id+"/_disable", nil) })
	deadline := time.Now().Add(90 * time.Second)
	for {
		st := esMust(t, http.MethodGet, kibana+"/api/alerting/rule/"+id, nil)["execution_status"].(map[string]any)
		last, _ := time.Parse(time.RFC3339Nano, fmt.Sprint(st["last_execution_date"]))
		if (st["status"] == "ok" || st["status"] == "active") && last.After(enabledAt) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("rule %s: no successful run 90 s after being enabled: %v", id, st)
		}
		time.Sleep(3 * time.Second)
	}
}

// activeAlerts renvoie les instances actives d'une règle (alertes en index).
func activeAlerts(t *testing.T, ruleID string) []string {
	t.Helper()
	status, out := esDo(t, http.MethodPost, esURL+"/.alerts-*/_search?expand_wildcards=open,hidden", map[string]any{
		"size": 200, "_source": false, "fields": []string{"kibana.alert.instance.id"},
		"query": map[string]any{"bool": map[string]any{"filter": []any{
			map[string]any{"term": map[string]any{"kibana.alert.rule.uuid": ruleID}},
			map[string]any{"term": map[string]any{"kibana.alert.status": "active"}},
		}}},
	})
	if status == http.StatusNotFound {
		return nil
	}
	if status >= 300 {
		t.Fatalf("alerts: HTTP %d: %v", status, out)
	}
	var ids []string
	for _, h := range out["hits"].(map[string]any)["hits"].([]any) {
		f, _ := h.(map[string]any)["fields"].(map[string]any)
		if v, ok := f["kibana.alert.instance.id"].([]any); ok && len(v) > 0 {
			ids = append(ids, fmt.Sprint(v[0]))
		}
	}
	return ids
}

func anyContains(ids []string, parts ...string) bool {
	for _, id := range ids {
		all := true
		for _, p := range parts {
			all = all && strings.Contains(id, p)
		}
		if all {
			return true
		}
	}
	return false
}

// Critère 13 : règles « composant muet » et « production arrêtée » du ndjson
// livré, importées dans Kibana 8.19, limitées aux clusters du test et
// exécutées toutes les 15 s. Trois agents : un arrêté, un dont le Kafka est en
// pause (cycles en échec), un témoin. Seuil de production abaissé à 30 s.
func TestC13WatchRules(t *testing.T) {
	b39 := newBench(t, kafka39)
	b44 := newBench(t, kafka44)
	b44.name += "-44"
	tDown, tMute := b39.topic("down", 1), b44.topic("mute", 1)
	tProd, tLive := b44.topic("prod", 1), b44.topic("live", 1)
	flags := []string{"--commit", "auto", "--commit-interval", "1s"}
	b39.consumer(b39.name+"-g", tDown, flags...)
	b44.consumer(b44.name+"-gm", tMute, flags...)
	b44.consumer(b44.name+"-gp", tProd+","+tLive, flags...)
	b39.producer(tDown, "steady:10:10m", true)
	b44.producer(tMute, "steady:10:10m", true)
	b44.producer(tProd, "steady:10:20s", false)
	b44.producer(tLive, "steady:10:10m", true)
	down := b39.agent(agentOpts{})
	mute := b44.agent(agentOpts{})
	live := b44.agent(agentOpts{})
	start := time.Now()
	for _, a := range []*agent{down, mute, live} {
		b39.waitFor("health of "+a.cluster, 60*time.Second, func() bool { return len(healthDocs(t, a.cluster, start)) >= 2 })
	}

	importExamples(t, kibanaURL)
	tuneRule(t, kibanaURL, ruleMute, func(p map[string]any) {
		q := fmt.Sprintf(`kafka.cluster.name : ("%s" or "%s" or "%s")`, down.cluster, mute.cluster, live.cluster)
		p["searchConfiguration"].(map[string]any)["query"].(map[string]any)["query"] = q
	})
	tuneRule(t, kibanaURL, ruleProduction, func(p map[string]any) {
		var q map[string]any
		json.Unmarshal([]byte(p["esQuery"].(string)), &q)
		filter := q["query"].(map[string]any)["bool"].(map[string]any)
		filter["filter"] = append(filter["filter"].([]any), map[string]any{"term": map[string]any{"kafka.cluster.name": live.cluster}})
		raw, _ := json.Marshal(q)
		p["esQuery"] = string(raw)
		p["threshold"] = []any{30}
	})
	// deux exécutions au moins avec les trois clusters vivants : la règle doit les avoir vus pour voir l'un disparaître
	time.Sleep(40 * time.Second)

	mute.stop()
	docker(t, "pause", "kafka39")
	pausedAt := time.Now()
	unpaused := false
	defer func() {
		if !unpaused {
			docker(t, "unpause", "kafka39")
		}
	}()
	var alerts []string
	b39.waitFor("\"silent component\" alerts for both clusters", 5*time.Minute, func() bool {
		alerts = activeAlerts(t, ruleMute)
		return anyContains(alerts, down.cluster) && anyContains(alerts, mute.cluster)
	})
	t.Logf("silent component: alerts %v after %s", alerts, time.Since(pausedAt).Round(time.Second))
	failed := 0
	for _, h := range healthDocs(t, down.cluster, pausedAt) {
		if h.boolean("kafkabeat.cycle_failed") {
			failed++
		}
	}
	docker(t, "unpause", "kafka39")
	unpaused = true
	if failed == 0 {
		t.Errorf("%s: no failed cycle reported while Kafka was paused", down.cluster)
	}
	if anyContains(activeAlerts(t, ruleMute), live.cluster) {
		t.Errorf("silent component: control cluster %s must not be alerting", live.cluster)
	}
	prod := activeAlerts(t, ruleProduction)
	t.Logf("production stopped: alerts %v", prod)
	if !anyContains(prod, live.cluster, tProd) {
		t.Errorf("production stopped: alert expected for %s/%s: %v", live.cluster, tProd, prod)
	}
	if anyContains(prod, tLive) {
		t.Errorf("production stopped: no alert expected for %s: %v", tLive, prod)
	}
}

// Critère 14 (Kafka 4.4) : topic à rétention (15 s) et segments (2 s) courts,
// consumer en pause : retention_used croît jusqu'à 1, puis
// commit_before_log_start est levé avec offsets_lost = log start − commit,
// et la paire est KO ; un topic compacté lu en retard ne signale aucun risque.
// Documents synthétiques : la perte seule rend KO, la part seule suffit au risque.
func TestC14LossRisk(t *testing.T) {
	b := newBench(t, kafka44)
	short := b.topicWith("short", 1, "retention.ms=15000,segment.ms=2000")
	compact := b.topicWith("compact", 1, "cleanup.policy=compact,segment.ms=2000")
	flags := []string{"--commit", "auto", "--commit-interval", "1s"}
	cShort := b.consumer(b.name+"-gs", short, flags...)
	cComp := b.consumer(b.name+"-gc", compact, append(flags, "--max-rate", "5")...)
	b.producer(short, "steady:50:10m", true)
	b.producer(compact, "steady:50:10m", true)
	a := b.agent(agentOpts{metricsInterval: 4 * time.Second})
	// pause une fois la rétention à l'œuvre (log start > 0) : la part se suit alors depuis un consumer à jour
	b.waitFor("retention start", 2*time.Minute, func() bool {
		docs := lagDocs(t, a.cluster, short, cShort.group, time.Time{})
		return len(docs) > 0 && lastDoc(docs).f("kafka.lag.retention_used") != nil
	})
	var compactLagging time.Time // premier document du topic compacté sorti de no_commit (début de la consommation)
	b.waitFor("compacted topic consumption", time.Minute, func() bool {
		for _, d := range lagDocs(t, a.cluster, compact, cComp.group, time.Time{}) {
			if d.str("kafka.lag.state") == "lagging" {
				compactLagging = d.ts
				return true
			}
		}
		return false
	})
	cShort.must(t, "POST", "/pause")
	pausedAt := time.Now()

	var loss doc
	b.waitFor("lost offsets", 3*time.Minute, func() bool {
		for _, d := range lagDocs(t, a.cluster, short, cShort.group, pausedAt) {
			if d.f("kafka.lag.offsets_lost") != nil {
				loss = d
				return true
			}
		}
		return false
	})
	if !loss.boolean("kafka.lag.flags.commit_before_log_start") || loss.num("kafka.lag.retention_used") != 1 {
		t.Fatalf("loss: expected flag and share at 1: %v", loss.flat)
	}
	var shares []float64
	for _, d := range lagDocs(t, a.cluster, short, cShort.group, pausedAt) {
		if d.ts.After(loss.ts) {
			break
		}
		if r, ok := d.f("kafka.lag.retention_used").(float64); ok {
			shares = append(shares, r)
		}
	}
	t.Logf("retention_used up to the loss: %v", shares)
	for i := 1; i < len(shares); i++ {
		if shares[i] < shares[i-1]-1e-9 {
			t.Errorf("retention_used decreases: %v then %v", shares[i-1], shares[i])
		}
	}
	if len(shares) < 3 || shares[0] >= 0.5 {
		t.Errorf("retention_used growth not observed: %v", shares)
	}
	// offsets perdus = log start lu au dernier cycle métriques (documents partition) − commit de la partition
	var oldest float64 = -1
	for _, d := range search(t, "kafkabeat-metrics", map[string]any{"kafka.cluster.name": a.cluster, "event.dataset": "kafka.partition",
		"kafka.topic.name": short}, pausedAt, 5000) {
		if !d.ts.After(loss.ts) {
			oldest = d.num("kafka.partition.offset.oldest")
		}
	}
	parts := loss.partitions()
	if len(parts) != 1 {
		t.Fatalf("expected one partition detail: %v", parts)
	}
	committed, _ := parts[0]["offset_committed"].(float64)
	if want := oldest - committed; loss.num("kafka.lag.offsets_lost") != want {
		t.Errorf("lost offsets %v, expected %v (log start %v − commit %v)", loss.num("kafka.lag.offsets_lost"), want, oldest, committed)
	}
	for _, d := range lagDocs(t, a.cluster, compact, cComp.group, pausedAt) {
		if d.f("kafka.lag.retention_used") != nil || d.boolean("kafka.lag.flags.commit_before_log_start") {
			t.Fatalf("compacted topic: no risk expected: %v", d.flat)
		}
	}
	// tuiles : fenêtre de 30 s sans les documents no_commit du démarrage du topic compacté
	time.Sleep(time.Until(compactLagging.Add(32 * time.Second)))
	cluster := map[string]any{"kafka.cluster.name": a.cluster}
	checkTiles(t, "short retention and compacted topic", tiles(t, cluster), map[string]float64{
		"Pairs at risk of loss": 1, "Consumption not OK": 1, "Consumption OK": 1,
	})

	synth := b.name + "-synth"
	risky := func(topic string, extra map[string]any) map[string]any {
		d := lagDoc(topic, "lagging", "Stable", 5.0, 1)
		for k, v := range extra {
			d["kafka"].(map[string]any)["lag"].(map[string]any)[k] = v
		}
		return d
	}
	indexLagDocs(t, synth, []map[string]any{
		risky("perte", map[string]any{"retention_used": 1.0, "offsets_lost": 42, "flags": map[string]any{"commit_before_log_start": true}}),
		risky("presque", map[string]any{"retention_used": 0.85}),
		risky("sain", map[string]any{"retention_used": 0.5}),
	})
	checkTiles(t, "synthetic documents", tiles(t, map[string]any{"kafka.cluster.name": synth}), map[string]float64{
		"Pairs at risk of loss": 2, "Consumption not OK": 1, "Consumption OK": 2,
	})
}

// lagTemplate lit le template installé du flux de lag : réglages et mapping.
func lagTemplate(t *testing.T, es, stream string) (map[string]any, map[string]any) {
	t.Helper()
	tpl := esMust(t, http.MethodGet, es+"/_index_template/"+stream, nil)["index_templates"].([]any)[0].(map[string]any)
	body := tpl["index_template"].(map[string]any)["template"].(map[string]any)
	return body["settings"].(map[string]any)["index"].(map[string]any), body["mappings"].(map[string]any)
}

// Critère 15 : TSDS. Deux agents sur le même cluster (cycle réécrit au même
// @timestamp) sans erreur ; Tendances lues sur un index sous-échantillonné daté
// de −10 j, sur 8.19 et 9.5 ; volume sur la rétention complète inférieur au
// classique 30 j, mesuré puis extrapolé.
func TestC15TimeSeries(t *testing.T) {
	t.Run("doublons", func(t *testing.T) {
		b := newBench(t, kafka39)
		topic := b.topic("t", 2)
		cons := b.consumer(b.name+"-g", topic, "--commit", "auto", "--commit-interval", "1s")
		b.producer(topic, "steady:20:10m", true)
		start := time.Now()
		a1 := b.agent(agentOpts{})
		b.agent(agentOpts{cluster: a1.cluster})
		b.waitDocs(a1.cluster, topic, cons.group, start, 10)
		time.Sleep(25 * time.Second)
		var dups float64
		for _, h := range healthDocs(t, a1.cluster, start) {
			if h.num("kafkabeat.sink.failed") != 0 || h.str("kafkabeat.sink.last_error") != "" {
				t.Fatalf("two agents on one cluster: publish error in health: %v", h.flat)
			}
			dups = max(dups, h.num("kafkabeat.sink.duplicates"))
		}
		if dups == 0 {
			t.Errorf("expected duplicates in the health of one of the two agents")
		}
		seen := map[time.Time]int{}
		for _, d := range lagDocs(t, a1.cluster, topic, cons.group, start) {
			seen[d.ts]++
			if seen[d.ts] > 1 {
				t.Fatalf("two documents of the same series at the same @timestamp: %s", d.ts)
			}
		}
		t.Logf("duplicates: %v counted without error, %d slots written once", dups, len(seen))
	})

	t.Run("tendances", func(t *testing.T) {
		b := newBench(t, kafka39)
		panels, dashboards := savedObjects(t)
		// un agent courant par Elasticsearch : templates v6 et data streams en TSDS
		start := time.Now()
		for _, es := range []string{esURL, es9URL} {
			b.agent(agentOpts{es: es})
		}
		b.waitFor("v6 templates installed by the agents", 60*time.Second, func() bool {
			for _, es := range []string{esURL, es9URL} {
				if status, _ := esDo(t, http.MethodGet, es+"/_index_template/kafkabeat-lag", nil); status != http.StatusOK {
					return false
				}
			}
			return true
		})
		for _, k := range []struct{ version, es, kibana string }{{"8.19", esURL, kibanaURL}, {"9.5", es9URL, kibana9URL}} {
			cluster := b.name + "-old-" + strings.ReplaceAll(k.version, ".", "")
			oldDownsampledLag(t, k.es, cluster, start.Add(-10*24*time.Hour))
			from, to := start.Add(-11*24*time.Hour), start
			rows := esql(t, k.es, panels["kafkabeat-trend-topics"].query, map[string]any{"kafka.cluster.name": cluster}, from, to)
			if len(rows) != 2 {
				t.Fatalf("Kibana %s, topics over the range: expected 2 rows: %v", k.version, rows)
			}
			for _, r := range rows {
				if v, _ := r["Messages produced"].(float64); v <= 0 {
					t.Errorf("Kibana %s: zero produced volume on the downsampled index: %v", k.version, r)
				}
			}
			if rows := esql(t, k.es, panels["kafkabeat-trend-lag-by-owner"].query, map[string]any{"kafka.cluster.name": cluster}, from, to); len(rows) == 0 {
				t.Errorf("Kibana %s: worst lag by team empty on the downsampled index", k.version)
			}
			importExamples(t, k.kibana)
			r := renderRange(t, k.kibana, "kafkabeat-trends", "now-11d", "now", filepath.Join(b.dir, "tendances-"+k.version+".png"),
				fmt.Sprintf(`kafka.cluster.name : "%s"`, cluster))
			t.Logf("Kibana %s, trends over 11 days: %d panels, load %d ms", k.version, r.Panels, r.LoadMs)
			if r.Panels != dashboards["kafkabeat-trends"] {
				t.Errorf("Kibana %s: %d panels rendered, %d expected", k.version, r.Panels, dashboards["kafkabeat-trends"])
			}
			for _, e := range r.Errors {
				t.Errorf("Kibana %s, panel %q: %s", k.version, e.Title, e.Message)
			}
		}
	})

	t.Run("volume", func(t *testing.T) {
		for _, s := range []struct {
			stream  string
			perHour float64 // documents bruts par heure et par série en production (10 s, 60 s)
		}{{"lag", 360}, {"metrics", 60}} {
			cl, ts := sizePerDoc(t, esURL, "kafkabeat-"+s.stream)
			classic30 := 30 * cl
			v3 := 7*ts + 83*ts/s.perHour
			t.Logf("volume %s (8.19): %.0f B/document classic, %.0f B/document TSDS (%+.0f %%); over retention, "+
				"v3 (7 d raw + 1 h up to 90 d) = %.2f × classic 30 d", s.stream, cl, ts, 100*(ts/cl-1), v3/classic30)
			if v3 >= classic30 {
				t.Errorf("%s: v3 volume over retention (%.0f) ≥ classic 30 d (%.0f)", s.stream, v3, classic30)
			}
		}
	})
}

// oldDownsampledLag écrit 24 h de documents de paire datés de start (deux
// topics, un toutes les 10 min) dans un index TSDS au mapping du flux de lag,
// le sous-échantillonne à 1 h et l'ajoute au data stream kafkabeat-lag ; retiré
// et supprimé en fin de test.
func oldDownsampledLag(t *testing.T, es, cluster string, start time.Time) {
	t.Helper()
	settings, mappings := lagTemplate(t, es, "kafkabeat-lag")
	raw, ds := cluster+"-brut", cluster+"-1h"
	t.Cleanup(func() {
		esDo(t, http.MethodPost, es+"/_data_stream/_modify", map[string]any{"actions": []any{
			map[string]any{"remove_backing_index": map[string]any{"data_stream": "kafkabeat-lag", "index": ds}}}})
		esDo(t, http.MethodDelete, es+"/"+raw, nil)
		esDo(t, http.MethodDelete, es+"/"+ds, nil)
	})
	esMust(t, http.MethodPut, es+"/"+raw, map[string]any{"mappings": mappings, "settings": map[string]any{
		"index.mode": "time_series", "index.routing_path": settings["routing_path"], "index.number_of_shards": 1,
		"index.mapping.total_fields.limit": 2000,
		"index.time_series.start_time":     start.UTC().Format(time.RFC3339),
		"index.time_series.end_time":       start.Add(24 * time.Hour).UTC().Format(time.RFC3339),
	}})
	var buf bytes.Buffer
	for i := 0; i < 144; i++ {
		ts := start.Add(time.Duration(i) * 10 * time.Minute)
		for j, topic := range []string{"t1", "t2"} {
			rate := float64(10 * (j + 1))
			d := lagDoc(topic, "lagging", "Stable", float64(5+i%7), float64(i%7))
			d["@timestamp"] = ts.UTC().Format(time.RFC3339)
			k := d["kafka"].(map[string]any)
			k["cluster"] = map[string]any{"name": cluster}
			k["topic"].(map[string]any)["owner"] = "recette"
			k["lag"].(map[string]any)["produce_rate"] = rate
			k["lag"].(map[string]any)["produced_total"] = int64(rate * 600 * float64(i))
			line, _ := json.Marshal(d)
			buf.WriteString(`{"create":{}}` + "\n" + string(line) + "\n")
		}
	}
	resp, err := http.Post(es+"/"+raw+"/_bulk?refresh=true", "application/x-ndjson", &buf)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 || bytes.Contains(data, []byte(`"errors":true`)) {
		t.Fatalf("old documents: HTTP %d: %.500s", resp.StatusCode, data)
	}
	esMust(t, http.MethodPut, es+"/"+raw+"/_block/write", nil)
	esMust(t, http.MethodPost, es+"/"+raw+"/_downsample/"+ds, map[string]any{"fixed_interval": "1h"})
	esMust(t, http.MethodPost, es+"/_data_stream/_modify", map[string]any{"actions": []any{
		map[string]any{"add_backing_index": map[string]any{"data_stream": "kafkabeat-lag", "index": ds}}}})
	n := esMust(t, http.MethodPost, es+"/"+ds+"/_count", nil)["count"].(float64)
	t.Logf("%s: 288 raw documents downsampled into %.0f", ds, n)
}

// sizePerDoc copie jusqu'à 200 000 documents des dernières 24 h d'un flux dans
// un index classique et un index TSDS au mapping installé, les fusionne en un
// segment et renvoie la taille par document de chacun.
func sizePerDoc(t *testing.T, es, stream string) (classic, tsds float64) {
	t.Helper()
	settings, mappings := lagTemplate(t, es, stream)
	n := esMust(t, http.MethodPost, es+"/"+stream+"/_count", map[string]any{"query": map[string]any{"range": map[string]any{"@timestamp": map[string]any{"gte": "now-24h"}}}})["count"].(float64)
	if n < 50000 {
		// le volume se mesure sur les documents de la simulation du critère 9 ;
		// make recette l'enchaîne avant, un lancement isolé ne l'a pas
		t.Skipf("%s: %.0f documents over 24 h, at least 50,000 expected: run TestC9OverviewUnderLoad first", stream, n)
	}
	plain := stripDimensions(mappings)
	base := map[string]any{"index.number_of_shards": 1, "index.number_of_replicas": 0, "index.codec": "best_compression", "index.mapping.total_fields.limit": 2000}
	now := time.Now()
	tsSettings := map[string]any{"index.mode": "time_series", "index.routing_path": settings["routing_path"],
		"index.time_series.start_time": now.Add(-25 * time.Hour).UTC().Format(time.RFC3339),
		"index.time_series.end_time":   now.Add(time.Hour).UTC().Format(time.RFC3339)}
	for k, v := range base {
		tsSettings[k] = v
	}
	out := map[string]float64{}
	for _, c := range []struct {
		name     string
		mapping  map[string]any
		settings map[string]any
	}{{runID + "-volume-classique", plain, base}, {runID + "-volume-tsds", mappings, tsSettings}} {
		esDo(t, http.MethodDelete, es+"/"+c.name, nil)
		t.Cleanup(func() { esDo(t, http.MethodDelete, es+"/"+c.name, nil) })
		esMust(t, http.MethodPut, es+"/"+c.name, map[string]any{"mappings": c.mapping, "settings": c.settings})
		r := esMust(t, http.MethodPost, es+"/_reindex?wait_for_completion=true&refresh=true", map[string]any{
			"max_docs": 200000, "conflicts": "proceed",
			"source": map[string]any{"index": stream, "size": 5000, "query": map[string]any{"range": map[string]any{"@timestamp": map[string]any{"gte": "now-24h"}}}},
			"dest":   map[string]any{"index": c.name, "op_type": "create"},
		})
		created := r["created"].(float64)
		esMust(t, http.MethodPost, es+"/"+c.name+"/_forcemerge?max_num_segments=1", nil)
		esMust(t, http.MethodPost, es+"/"+c.name+"/_refresh", nil)
		esMust(t, http.MethodPost, es+"/"+c.name+"/_flush?force=true", nil)
		time.Sleep(3 * time.Second)
		esMust(t, http.MethodPost, es+"/"+c.name+"/_refresh", nil)
		resp, err := http.Get(es + "/_cat/segments/" + c.name + "?format=json&bytes=b")
		if err != nil {
			t.Fatal(err)
		}
		var segs []struct {
			Size string `json:"size"`
		}
		json.NewDecoder(resp.Body).Decode(&segs)
		resp.Body.Close()
		var size float64
		for _, s := range segs {
			var v float64
			fmt.Sscan(s.Size, &v)
			size += v
		}
		out[c.name] = size / created
		t.Logf("%s: %.0f documents, %d segment(s), %.1f MB", c.name, created, len(segs), size/1e6)
	}
	return out[runID+"-volume-classique"], out[runID+"-volume-tsds"]
}

// stripDimensions copie un mapping sans les marques de dimension (index classique).
func stripDimensions(m map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		if k == "time_series_dimension" {
			continue
		}
		if sub, ok := v.(map[string]any); ok {
			out[k] = stripDimensions(sub)
		} else {
			out[k] = v
		}
	}
	return out
}

// Critère 16 : sur 4.4, un groupe au protocole consumer (KIP-848) à côté d'un
// groupe classique : lui seul compté dans la tuile et listé ; sur 3.9 (franz-go
// revient au protocole classique), tuile à 0.
func TestC16Kip848Groups(t *testing.T) {
	panels, _ := savedObjects(t)
	for _, k := range []struct {
		version, kafka string
		want           float64
	}{{"4.4", kafka44, 1}, {"3.9", kafka39, 0}} {
		t.Run(k.version, func(t *testing.T) {
			b := newBench(t, k.kafka)
			topic := b.topic("t", 4)
			flags := []string{"--commit", "auto", "--commit-interval", "1s"}
			kip := b.consumer(b.name+"-gnew", topic, append(flags, "--server-side")...)
			old := b.consumer(b.name+"-gold", topic, flags...)
			b.producer(topic, "steady:20:10m", true)
			a := b.agent(agentOpts{})
			newest := lastDoc(b.waitDocs(a.cluster, topic, kip.group, time.Time{}, 3))
			oldest := lastDoc(b.waitDocs(a.cluster, topic, old.group, time.Time{}, 3))
			wantType := map[bool]string{true: "consumer", false: "classic"}[k.want == 1]
			if got := newest.str("kafka.consumergroup.type"); got != wantType {
				t.Errorf("--server-side group: type %q, expected %q", got, wantType)
			}
			if got := oldest.str("kafka.consumergroup.type"); got != "classic" {
				t.Errorf("classic group: type %q", got)
			}
			cluster := map[string]any{"kafka.cluster.name": a.cluster}
			checkTiles(t, "Kafka "+k.version, tiles(t, cluster), map[string]float64{"KIP-848 groups": k.want})
			now := time.Now()
			rows := esql(t, esURL, panels["kafkabeat-kip848-groups"].query, cluster, now.Add(-15*time.Minute), now)
			if len(rows) != int(k.want) || (k.want == 1 && rows[0]["Group"] != kip.group) {
				t.Errorf("KIP-848 group list: %v", rows)
			}
		})
	}
}
