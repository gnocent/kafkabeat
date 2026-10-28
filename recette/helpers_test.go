//go:build bench

package recette

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var (
	kafka39  = env("KB_KAFKA39", "localhost:19092")
	kafka44  = env("KB_KAFKA44", "localhost:19094")
	esURL    = env("KB_ES", "http://localhost:9200")
	binDir   string
	interval = 2 * time.Second
	runID    = fmt.Sprintf("r%d", time.Now().Unix()%100000)
	nextPort = 18100
	goBin    = env("GO", "go")
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "kafkabeat-recette-")
	if err != nil {
		panic(err)
	}
	binDir = dir
	for _, b := range [][2]string{{"kafkabeat", "../cmd/kafkabeat"}, {"harness", "../bench/harness"}} {
		cmd := exec.Command(goBin, "build", "-o", filepath.Join(dir, b[0]), b[1])
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n", b[0], err)
			os.Exit(1)
		}
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// bench est le contexte d'un test : un cluster, des noms uniques, des
// processus à arrêter en fin de test.
type bench struct {
	t      *testing.T
	kafka  string
	name   string // préfixe unique des topics, groupes et du nom de cluster
	dir    string
	procs  []*exec.Cmd
	nextID int
	topics []string // topics créés, supprimés en fin de test
	groups []string // groupes des consumers lancés, supprimés en fin de test
}

func newBench(t *testing.T, kafka string) *bench {
	t.Helper()
	name := strings.ToLower(strings.TrimPrefix(t.Name(), "Test"))
	name = strings.NewReplacer("/", "-", "_", "-", " ", "-").Replace(name)
	b := &bench{t: t, kafka: kafka, name: runID + "-" + name, dir: t.TempDir()}
	// nettoyages dans l'ordre inverse : processus arrêtés (les consumers
	// quittent leur groupe), puis groupes et topics supprimés du banc
	t.Cleanup(b.deleteAll)
	t.Cleanup(b.stopAll)
	return b
}

func (b *bench) topic(suffix string, partitions int) string {
	name := b.name + "-" + suffix
	b.harness("topics", "create", "--name", name, "--partitions", fmt.Sprint(partitions))
	b.topics = append(b.topics, name)
	return name
}

// deleteAll supprime du banc les groupes et topics du test ; un échec n'est
// que journalisé (groupe déjà supprimé, topic recréé sous un autre nom...).
// Sans ce nettoyage, chaque passage laissait une trentaine de topics par
// cluster, et les relevés Metricbeat qu'en tire le critère 11 finissaient
// par sortir de sa fenêtre de recherche.
func (b *bench) deleteAll() {
	for _, g := range b.groups {
		b.harnessQuiet("groups", "delete", "--group", g)
	}
	for _, t := range b.topics {
		b.harnessQuiet("topics", "delete", "--name", t)
	}
}

// harnessQuiet est harness sans échec du test : l'erreur est journalisée.
func (b *bench) harnessQuiet(args ...string) {
	full := []string{args[0], args[1], "--bootstrap", b.kafka}
	full = append(full, args[2:]...)
	cmd := exec.Command(filepath.Join(binDir, "harness"), full...)
	if out, err := cmd.CombinedOutput(); err != nil {
		b.t.Logf("cleanup: harness %v: %v\n%s", args, err, out)
	}
}

// harness exécute « harness <sous-commande> <action> --bootstrap ... <options> ».
func (b *bench) harness(args ...string) string {
	b.t.Helper()
	full := []string{args[0], args[1], "--bootstrap", b.kafka}
	full = append(full, args[2:]...)
	cmd := exec.Command(filepath.Join(binDir, "harness"), full...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		b.t.Fatalf("harness %v: %v\n%s", args, err, out.String())
	}
	return out.String()
}

// harnessBg lance une commande du harnais en arrière-plan.
func (b *bench) harnessBg(args ...string) *exec.Cmd {
	b.t.Helper()
	full := append([]string{args[0], "--bootstrap", b.kafka}, args[1:]...)
	cmd := exec.Command(filepath.Join(binDir, "harness"), full...)
	logf, _ := os.Create(filepath.Join(b.dir, fmt.Sprintf("%s-%d.log", args[0], len(b.procs))))
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		b.t.Fatalf("harness %v: %v", args, err)
	}
	b.procs = append(b.procs, cmd)
	return cmd
}

// producer démarre un producteur avec journal et renvoie le chemin du journal.
func (b *bench) producer(topic, phases string, repeat bool) (*exec.Cmd, string) {
	journal := filepath.Join(b.dir, topic+".journal")
	args := []string{"produce", "--topic", topic, "--phases", phases, "--journal", journal}
	if repeat {
		args = append(args, "--repeat")
	}
	return b.harnessBg(args...), journal
}

type consumer struct {
	cmd     *exec.Cmd
	control string
	group   string
}

// quarantinePartition est la partition de __consumer_offsets sacrifiée aux
// injections du critère 5 : aucun autre groupe du banc ne doit y tomber.
const quarantinePartition = 49

// groupOn dérive de base un nom de groupe dont le coordinateur est la
// partition want de __consumer_offsets (50 partitions sur le banc).
func groupOn(base string, want int32) string {
	for i := 0; ; i++ {
		name := fmt.Sprintf("%s-%d", base, i)
		if javaHashPartition(name, 50) == want {
			return name
		}
	}
}

// groupOff dérive de base un nom de groupe hors de la partition avoid.
func groupOff(base string, avoid int32) string {
	name := base
	for i := 0; javaHashPartition(name, 50) == avoid; i++ {
		name = fmt.Sprintf("%s-%d", base, i)
	}
	return name
}

// consumer démarre un consommateur pilotable ; le nom du groupe est déplacé
// hors de la partition de quarantaine.
func (b *bench) consumer(group, topics string, extra ...string) *consumer {
	return b.consumerRaw(groupOff(group, quarantinePartition), topics, extra...)
}

// consumerRaw démarre un consommateur avec le nom de groupe tel quel.
func (b *bench) consumerRaw(group, topics string, extra ...string) *consumer {
	port := nextPort
	nextPort++
	control := fmt.Sprintf("127.0.0.1:%d", port)
	args := append([]string{"consume", "--group", group, "--topics", topics, "--control", control}, extra...)
	b.groups = append(b.groups, group)
	cmd := b.harnessBg(args...)
	c := &consumer{cmd: cmd, control: control, group: group}
	b.waitFor("consumer "+group, 30*time.Second, func() bool {
		_, err := c.call(http.MethodGet, "/status")
		return err == nil
	})
	return c
}

func (c *consumer) call(method, path string) (string, error) {
	req, _ := http.NewRequest(method, "http://"+c.control+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return string(body), fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, body)
	}
	return string(body), nil
}

func (c *consumer) must(t *testing.T, method, path string) string {
	t.Helper()
	out, err := c.call(method, path)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// stop interrompt le consommateur proprement (commit final puis sortie du groupe).
func (c *consumer) stop() {
	c.cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { c.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		c.cmd.Process.Kill()
	}
}

func stopCmd(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		cmd.Process.Kill()
	}
}

func (b *bench) stopAll() {
	for i := len(b.procs) - 1; i >= 0; i-- {
		stopCmd(b.procs[i])
	}
}

// agent est une instance de kafkabeat lancée par le test.
type agent struct {
	b        *bench
	cmd      *exec.Cmd
	cluster  string
	cfgPath  string
	stateDir string
	logPath  string
}

type agentOpts struct {
	snapshot        time.Duration
	reconcile       time.Duration
	metricsInterval time.Duration
	es              string      // autre Elasticsearch (état sur disque séparé) ; défaut esURL
	owners          [][2]string // règles output.owners : motif, propriétaire
	commits         string      // collect.commits.mode ; défaut hybrid
	jolokiaPort     int         // metricset broker sur ce port (0 : désactivé)
	cluster         string      // nom de cluster imposé (deux agents sur le même cluster) ; défaut : nom unique
}

func (b *bench) agent(o agentOpts) *agent {
	b.t.Helper()
	// un répertoire d'état par agent : deux agents d'un même test ne doivent
	// jamais partager state.gob et history.snap
	stateDir := filepath.Join(b.dir, fmt.Sprintf("state-%d", b.nextID+1))
	if o.es == "" {
		o.es = esURL
	}
	owners := "[]"
	if len(o.owners) > 0 {
		var rules []string
		for _, r := range o.owners {
			rules = append(rules, fmt.Sprintf("{topic: %q, owner: %q}", r[0], r[1]))
		}
		owners = "[" + strings.Join(rules, ", ") + "]"
	}
	if o.snapshot == 0 {
		o.snapshot = 10 * time.Second
	}
	if o.reconcile == 0 {
		o.reconcile = 30 * time.Second
	}
	if o.metricsInterval == 0 {
		o.metricsInterval = 20 * time.Second
	}
	if o.commits == "" {
		o.commits = "hybrid"
	}
	jolokia := "{enabled: false}"
	if o.jolokiaPort > 0 {
		jolokia = fmt.Sprintf("{enabled: true, port: %d, timeout: 5s}", o.jolokiaPort)
	}
	b.nextID++
	cluster := fmt.Sprintf("%s-%d", b.name, b.nextID)
	if o.cluster != "" {
		cluster = o.cluster
	}
	cfg := fmt.Sprintf(`cluster: %s
kafka:
  brokers: [%q]
collect:
  interval: %s
  metrics_interval: %s
  topics: {include: [%q]}
  groups: {include: [%q]}
  stalled_after: 10s
  flag_hold_cycles: 3
  gone_cycles: 3
  smoothing_window: 10s
  commits: {mode: %s, reconcile_interval: %s}
history:
  tiers:
    - {step: 2s, span: 15m}
    - {step: 1m, span: 3h}
    - {step: 10m, span: 168h}
state: {path: %q, snapshot_interval: %s}
output:
  elasticsearch: {urls: [%q], setup: true}
  fields: {recette: %q}
  owners: %s
jolokia: %s
`, cluster, b.kafka, interval, o.metricsInterval, b.name+"-*", b.name+"-*",
		o.commits, o.reconcile, stateDir, o.snapshot, o.es, runID, owners, jolokia)
	path := filepath.Join(b.dir, fmt.Sprintf("%s-%d.yml", cluster, b.nextID))
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		b.t.Fatal(err)
	}
	a := &agent{b: b, cluster: cluster, cfgPath: path, stateDir: stateDir, logPath: filepath.Join(b.dir, fmt.Sprintf("%s-%d.log", cluster, b.nextID))}
	a.start()
	return a
}

func (a *agent) start() {
	a.b.t.Helper()
	cmd := exec.Command(filepath.Join(binDir, "kafkabeat"), "-config", a.cfgPath)
	logf, err := os.OpenFile(a.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		a.b.t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		a.b.t.Fatal(err)
	}
	a.cmd = cmd
	a.b.procs = append(a.b.procs, cmd)
}

// stop arrête l'agent par SIGTERM (snapshot final) et attend sa sortie.
func (a *agent) stop() {
	stopCmd(a.cmd)
}

func (a *agent) log() string {
	data, _ := os.ReadFile(a.logPath)
	return string(data)
}

// doc est un document Elasticsearch aplati pour les assertions.
type doc struct {
	ts   time.Time
	src  map[string]any
	flat map[string]any
}

func (d doc) f(path string) any { return d.flat[path] }

func (d doc) num(path string) float64 {
	if v, ok := d.flat[path].(float64); ok {
		return v
	}
	return -1
}

// numOK distingue un champ absent d'une valeur (−1 est un offset légitime).
func (d doc) numOK(path string) (float64, bool) {
	v, ok := d.flat[path].(float64)
	return v, ok
}

func (d doc) str(path string) string {
	s, _ := d.flat[path].(string)
	return s
}

func (d doc) boolean(path string) bool {
	v, _ := d.flat[path].(bool)
	return v
}

// partitions renvoie le détail des partitions d'un document de lag.
func (d doc) partitions() []map[string]any {
	lag, _ := d.src["kafka"].(map[string]any)["lag"].(map[string]any)
	raw, _ := lag["partitions"].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, p := range raw {
		out = append(out, p.(map[string]any))
	}
	return out
}

func flatten(prefix string, v any, out map[string]any) {
	m, ok := v.(map[string]any)
	if !ok {
		out[prefix] = v
		return
	}
	for k, val := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		if _, isMap := val.(map[string]any); isMap {
			flatten(key, val, out)
		} else {
			out[key] = val
		}
	}
}

// search interroge un data stream avec des termes exacts, trié par @timestamp.
func search(t *testing.T, stream string, terms map[string]any, since time.Time, size int) []doc {
	t.Helper()
	var must []map[string]any
	for k, v := range terms {
		must = append(must, map[string]any{"term": map[string]any{k: v}})
	}
	if !since.IsZero() {
		must = append(must, map[string]any{"range": map[string]any{"@timestamp": map[string]any{"gte": since.UTC().Format(time.RFC3339Nano)}}})
	}
	query := map[string]any{
		"size":  size,
		"sort":  []map[string]any{{"@timestamp": "asc"}},
		"query": map[string]any{"bool": map[string]any{"filter": must}},
	}
	body, _ := json.Marshal(query)
	req, _ := http.NewRequest(http.MethodPost, esURL+"/"+stream+"/_search", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("elasticsearch: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode >= 300 {
		t.Fatalf("elasticsearch: HTTP %d: %s", resp.StatusCode, data)
	}
	var r struct {
		Hits struct {
			Hits []struct {
				Source map[string]any `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	docs := make([]doc, 0, len(r.Hits.Hits))
	for _, h := range r.Hits.Hits {
		flat := make(map[string]any)
		flatten("", h.Source, flat)
		ts, _ := time.Parse(time.RFC3339Nano, flat["@timestamp"].(string))
		docs = append(docs, doc{ts: ts, src: h.Source, flat: flat})
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].ts.Before(docs[j].ts) })
	return docs
}

func lagDocs(t *testing.T, cluster, topic, group string, since time.Time) []doc {
	return search(t, "kafkabeat-lag", map[string]any{"kafka.cluster.name": cluster, "kafka.topic.name": topic, "kafka.consumergroup.id": group}, since, 5000)
}

func healthDocs(t *testing.T, cluster string, since time.Time) []doc {
	return search(t, "kafkabeat-health", map[string]any{"kafka.cluster.name": cluster}, since, 5000)
}

func (b *bench) waitFor(what string, timeout time.Duration, cond func() bool) {
	b.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(interval / 2)
	}
	b.t.Fatalf("waiting for %s: timed out after %s", what, timeout)
}

// waitDocs attend qu'au moins n documents de lag existent pour la paire.
func (b *bench) waitDocs(cluster, topic, group string, since time.Time, n int) []doc {
	b.t.Helper()
	var docs []doc
	b.waitFor(fmt.Sprintf("%d lag documents for %s/%s", n, topic, group), 60*time.Second, func() bool {
		docs = lagDocs(b.t, cluster, topic, group, since)
		return len(docs) >= n
	})
	return docs
}

func docker(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "docker", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker %v: %v\n%s", args, err, out)
	}
}

func lastDoc(docs []doc) doc {
	return docs[len(docs)-1]
}

// gaps vérifie qu'aucun trou supérieur à maxGap n'existe entre documents consécutifs.
func gaps(docs []doc, maxGap time.Duration) []string {
	var out []string
	for i := 1; i < len(docs); i++ {
		if d := docs[i].ts.Sub(docs[i-1].ts); d > maxGap {
			out = append(out, fmt.Sprintf("%s gap between %s and %s", d, docs[i-1].ts.Format(time.TimeOnly), docs[i].ts.Format(time.TimeOnly)))
		}
	}
	return out
}
