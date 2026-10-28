// Package jolokia remplace le metricset broker de Metricbeat : sur chaque
// broker découvert par les métadonnées, il lit par Jolokia les mêmes MBeans
// et produit les mêmes documents (kafka.broker.*), à l'intervalle des
// métriques et hors du cycle de 10 s. Un broker injoignable n'empêche pas
// les autres d'être émis ; il est signalé dans le document de santé.
package jolokia

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gnocent/kafkabeat/internal/kafkaclient"
	"github.com/gnocent/kafkabeat/internal/sink"
)

type mapping struct {
	mbean, attr, field string
}

// mappings reprend le manifeste du metricset broker de Metricbeat 8.19
// (module/kafka/broker/manifest.yml), dans son ordre. Comme Metricbeat, le
// champ d'un couple MBean + attribut est celui de la dernière ligne qui le
// cite : FailedProduceRequestsPerSec et FailedFetchRequestsPerSec ne
// produisent que request.produce.failed et request.fetch.failed.
var mappings = []mapping{
	{"kafka.network:type=RequestChannel,name=RequestQueueSize", "Value", "request.channel.queue.size"},
	{"kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec", "OneMinuteRate", "request.produce.failed_per_second"},
	{"kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec", "OneMinuteRate", "request.fetch.failed_per_second"},
	{"kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec", "OneMinuteRate", "request.produce.failed"},
	{"kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec", "OneMinuteRate", "request.fetch.failed"},
	{"kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs", "OneMinuteRate", "replication.leader_elections"},
	{"kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec", "OneMinuteRate", "replication.unclean_leader_elections"},
	{"kafka.server:type=SessionExpireListener,name=ZooKeeperDisconnectsPerSec", "OneMinuteRate", "session.zookeeper.disconnect"},
	{"kafka.server:type=SessionExpireListener,name=ZooKeeperExpiresPerSec", "OneMinuteRate", "session.zookeeper.expire"},
	{"kafka.server:type=SessionExpireListener,name=ZooKeeperReadOnlyConnectsPerSec", "OneMinuteRate", "session.zookeeper.readonly"},
	{"kafka.server:type=SessionExpireListener,name=ZooKeeperSyncConnectsPerSec", "OneMinuteRate", "session.zookeeper.sync"},
	{"kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs", "OneMinuteRate", "log.flush_rate"},
	{"kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec,topic=*", "OneMinuteRate", "topic.net.rejected.bytes_per_sec"},
	{"kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=*", "OneMinuteRate", "topic.net.in.bytes_per_sec"},
	{"kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=*", "OneMinuteRate", "topic.net.out.bytes_per_sec"},
	{"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*", "OneMinuteRate", "topic.messages_in"},
	{"kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec", "OneMinuteRate", "net.rejected.bytes_per_sec"},
	{"kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec", "OneMinuteRate", "net.in.bytes_per_sec"},
	{"kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec", "OneMinuteRate", "net.out.bytes_per_sec"},
	{"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec", "OneMinuteRate", "messages_in"},
}

type request struct {
	Type      string   `json:"type"`
	MBean     string   `json:"mbean"`
	Attribute []string `json:"attribute"`
}

// requests renvoie une lecture par couple MBean + attribut distinct et le
// champ de chacune (dernière ligne du manifeste qui le cite).
func requests() ([]request, []string) {
	field := make(map[[2]string]string)
	var order [][2]string
	for _, m := range mappings {
		k := [2]string{m.mbean, m.attr}
		if _, ok := field[k]; !ok {
			order = append(order, k)
		}
		field[k] = m.field
	}
	reqs := make([]request, len(order))
	fields := make([]string, len(order))
	for i, k := range order {
		reqs[i] = request{Type: "read", MBean: k[0], Attribute: []string{k[1]}}
		fields[i] = field[k]
	}
	return reqs, fields
}

// Options règle l'accès aux agents Jolokia des brokers.
type Options struct {
	Port     int
	Path     string
	Username string
	Password string
	TLS      *tls.Config // nil : http
	Timeout  time.Duration
	Period   time.Duration // intervalle des métriques
	Cluster  string
	Fields   map[string]string // enrichissement statique
	Owner    func(topic string) string
}

// Brokers renvoie l'identifiant du cluster et ses brokers (métadonnées).
type Brokers func(ctx context.Context) (string, []kafkaclient.BrokerMeta, error)

// Poller interroge les brokers et publie les documents.
type Poller struct {
	opts    Options
	brokers Brokers
	out     sink.Sink
	client  *http.Client
	logger  *log.Logger
	// url construit l'adresse Jolokia d'un broker (remplacée par les tests)
	url func(b kafkaclient.BrokerMeta) string

	mu   sync.Mutex
	last status
}

type status struct {
	at       time.Time
	brokers  int
	failed   int
	docs     int
	duration time.Duration
	errors   []string
}

// New crée le poller.
func New(opts Options, brokers Brokers, out sink.Sink, logger *log.Logger) *Poller {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	scheme := "http"
	if opts.TLS != nil {
		tr.TLSClientConfig = opts.TLS
		scheme = "https"
	}
	p := &Poller{opts: opts, brokers: brokers, out: out, logger: logger, client: &http.Client{Transport: tr}}
	path := "/" + strings.Trim(opts.Path, "/") + "/"
	p.url = func(b kafkaclient.BrokerMeta) string {
		return scheme + "://" + hostPort(b.Host, opts.Port) + path
	}
	return p
}

// Run interroge les brokers au démarrage puis sur la grille de l'intervalle
// des métriques, jusqu'à annulation.
func (p *Poller) Run(ctx context.Context) {
	for {
		p.Poll(ctx, time.Now())
		next := time.Now().Truncate(p.opts.Period).Add(p.opts.Period)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(next)):
		}
	}
}

// Poll interroge tous les brokers en parallèle, en Timeout au plus, et
// publie les documents des brokers qui ont répondu.
func (p *Poller) Poll(ctx context.Context, now time.Time) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, p.opts.Timeout)
	defer cancel()
	st := status{at: now}
	clusterID, brokers, err := p.brokers(ctx)
	if err != nil {
		st.errors = []string{fmt.Sprintf("jolokia: broker metadata: %v", err)}
		p.record(st, start)
		return
	}
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].ID < brokers[j].ID })
	ts := now.Truncate(10 * time.Second).UTC().Format(time.RFC3339Nano)
	docs := make([][]sink.Doc, len(brokers))
	errs := make([]error, len(brokers))
	var wg sync.WaitGroup
	for i, b := range brokers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			docs[i], errs[i] = p.read(ctx, b, clusterID, ts)
		}()
	}
	wg.Wait()
	var all []sink.Doc
	for i, b := range brokers {
		if errs[i] != nil {
			st.failed++
			st.errors = append(st.errors, fmt.Sprintf("jolokia: broker %d (%s): %v", b.ID, p.url(b), errs[i]))
			continue
		}
		all = append(all, docs[i]...)
	}
	st.brokers, st.docs = len(brokers), len(all)
	if len(all) > 0 {
		if err := p.out.Emit(all); err != nil {
			p.logger.Printf("jolokia: sending documents: %v", err)
		}
	}
	for _, e := range st.errors {
		p.logger.Print(e)
	}
	p.record(st, start)
}

func (p *Poller) record(st status, start time.Time) {
	st.duration = time.Since(start)
	p.mu.Lock()
	p.last = st
	p.mu.Unlock()
}

// Health renvoie la section jolokia du document de santé et les erreurs de
// la dernière interrogation.
func (p *Poller) Health() (map[string]any, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.last.at.IsZero() {
		return nil, nil
	}
	return map[string]any{
		"brokers":        p.last.brokers,
		"brokers_failed": p.last.failed,
		"docs":           p.last.docs,
		"duration_ms":    p.last.duration.Milliseconds(),
		"last_poll":      p.last.at.UTC().Format(time.RFC3339Nano),
	}, p.last.errors
}

type entry struct {
	Value  json.RawMessage `json:"value"`
	Status int             `json:"status"`
}

// maxResponseBytes borne la lecture d'une réponse Jolokia (quelques dizaines
// de kilooctets par broker en pratique).
const maxResponseBytes = 16 << 20

// read interroge un broker (requête groupée, comme Metricbeat) et construit
// ses documents : un pour les MBeans fixes, un par MBean trouvé par joker
// (kafka.broker.mbean). Les MBeans absents (404, ZooKeeper en KRaft) et les
// valeurs non numériques sont ignorés.
func (p *Poller) read(ctx context.Context, b kafkaclient.BrokerMeta, clusterID, ts string) ([]sink.Doc, error) {
	reqs, fields := requests()
	body, err := json.Marshal(reqs)
	if err != nil {
		return nil, err
	}
	// Noms canoniques (clés triées) : ceux que publie Metricbeat, constaté sur
	// le banc (son chemin « ?ignoreErrors=true&canonicalNaming=false » arrive
	// échappé). Sans ignoreErrors, un MBean absent revient en 404 et est
	// ignoré, au lieu d'un objet d'erreur à la place de la valeur.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url(b)+"?canonicalNaming=true", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.opts.Username != "" {
		req.SetBasicAuth(p.opts.Username, p.opts.Password)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var entries []entry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("unreadable response: %w", err)
	}
	if len(entries) != len(reqs) {
		return nil, fmt.Errorf("%d responses for %d reads", len(entries), len(reqs))
	}

	fixed := map[string]any{}
	perMBean := map[string]map[string]any{}
	for i, e := range entries {
		if e.Status != http.StatusOK || len(e.Value) == 0 {
			continue
		}
		attr := reqs[i].Attribute[0]
		if !strings.Contains(reqs[i].MBean, "*") {
			var v map[string]any
			if json.Unmarshal(e.Value, &v) == nil {
				if n, ok := v[attr].(float64); ok {
					put(fixed, fields[i], n)
				}
			}
			continue
		}
		var byName map[string]map[string]any
		if json.Unmarshal(e.Value, &byName) != nil {
			continue
		}
		for name, v := range byName {
			n, ok := v[attr].(float64)
			if !ok {
				continue
			}
			if perMBean[name] == nil {
				perMBean[name] = map[string]any{"mbean": name}
			}
			put(perMBean[name], fields[i], n)
		}
	}

	var docs []sink.Doc
	if len(fixed) > 0 {
		docs = append(docs, p.doc(b, clusterID, ts, fixed, ""))
	}
	names := make([]string, 0, len(perMBean))
	for name := range perMBean {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		docs = append(docs, p.doc(b, clusterID, ts, perMBean[name], mbeanProperty(name, "topic")))
	}
	return docs, nil
}

func (p *Poller) doc(b kafkaclient.BrokerMeta, clusterID, ts string, broker map[string]any, topic string) sink.Doc {
	broker["id"] = b.ID
	broker["address"] = hostPort(b.Host, int(b.Port))
	kafka := map[string]any{
		"cluster": map[string]any{"name": p.opts.Cluster, "id": clusterID},
		"broker":  broker,
	}
	if topic != "" {
		t := map[string]any{"name": topic}
		if p.opts.Owner != nil {
			if o := p.opts.Owner(topic); o != "" {
				t["owner"] = o
			}
		}
		kafka["topic"] = t
	}
	body := map[string]any{
		"@timestamp": ts,
		"event":      map[string]any{"dataset": "kafka.broker", "module": "kafka"},
		"metricset":  map[string]any{"name": "broker", "period": p.opts.Period.Milliseconds()},
		"service":    map[string]any{"type": "kafka", "address": hostPort(b.Host, p.opts.Port)},
		"kafka":      kafka,
	}
	for k, v := range p.opts.Fields {
		body[k] = v
	}
	return sink.Doc{Stream: "metrics", Body: body}
}

// hostPort assemble hôte et port, en encadrant une adresse IPv6 littérale.
func hostPort(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// put range v sous le chemin pointé (request.channel.queue.size).
func put(m map[string]any, dotted string, v any) {
	parts := strings.Split(dotted, ".")
	for _, k := range parts[:len(parts)-1] {
		next, ok := m[k].(map[string]any)
		if !ok {
			next = map[string]any{}
			m[k] = next
		}
		m = next
	}
	m[parts[len(parts)-1]] = v
}

// mbeanProperty lit une propriété d'un nom de MBean (domaine:clé=valeur,...).
func mbeanProperty(name, key string) string {
	_, props, ok := strings.Cut(name, ":")
	if !ok {
		return ""
	}
	for _, kv := range strings.Split(props, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok && k == key {
			return strings.Trim(v, `"`)
		}
	}
	return ""
}
