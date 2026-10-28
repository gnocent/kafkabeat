package sink

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ESOptions configure la sortie Elasticsearch.
type ESOptions struct {
	URLs        []string
	Username    string
	Password    string
	APIKey      string
	TLS         *tls.Config
	Prefix      string
	BulkMaxDocs int
}

// ES publie en bulk dans les data streams <prefix>-<stream>.
type ES struct {
	opts   ESOptions
	client *http.Client
	logger *log.Logger

	setupMu sync.Mutex // une seule installation à la fois (collecteur et metricset broker émettent en parallèle)

	mu          sync.Mutex
	next        int // index de l'URL courante
	sent        int64
	failed      int64
	duplicates  int64
	requests    int64
	lastError   string
	setupWanted bool
	setupDone   bool
}

// NewES crée la sortie ; WantSetup y ajoute l'installation des templates au
// premier envoi.
func NewES(opts ESOptions, logger *log.Logger) (*ES, error) {
	if len(opts.URLs) == 0 {
		return nil, errors.New("elasticsearch: no URL")
	}
	if opts.BulkMaxDocs <= 0 {
		opts.BulkMaxDocs = 2000
	}
	for i, u := range opts.URLs {
		opts.URLs[i] = strings.TrimRight(u, "/")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if opts.TLS != nil {
		tr.TLSClientConfig = opts.TLS
	}
	return &ES{opts: opts, client: &http.Client{Transport: tr, Timeout: 30 * time.Second}, logger: logger}, nil
}

// Stream renvoie le nom du data stream d'un flux.
func (e *ES) Stream(stream string) string { return e.opts.Prefix + "-" + stream }

// WantSetup demande l'installation des templates avant le premier envoi ;
// elle est retentée à chaque envoi tant qu'elle échoue.
func (e *ES) WantSetup() {
	e.mu.Lock()
	e.setupWanted = true
	e.mu.Unlock()
}

// EnsureSetup installe les templates maintenant si WantSetup l'a demandé et
// que ce n'est pas encore fait : au démarrage, avant le premier cycle, pour
// qu'une installation longue (Elasticsearch froid, nouvelle version de
// template) ne retarde pas la grille des cycles. En cas d'échec, elle est
// retentée à chaque envoi.
func (e *ES) EnsureSetup(ctx context.Context) error {
	e.setupMu.Lock()
	defer e.setupMu.Unlock()
	e.mu.Lock()
	need := e.setupWanted && !e.setupDone
	e.mu.Unlock()
	if !need {
		return nil
	}
	if err := e.Setup(ctx); err != nil {
		return fmt.Errorf("installing templates: %w", err)
	}
	e.mu.Lock()
	e.setupDone = true
	e.mu.Unlock()
	return nil
}

// Emit envoie les documents par lots d'au plus BulkMaxDocs ; une erreur de
// transport fait essayer les URL suivantes (voir do). Quand aucune URL ne
// répond, les lots restants sont abandonnés sans essai : un Elasticsearch
// qui ne répond plus ne retient pas le cycle d'un délai par lot (des dizaines
// de lots à 30 000 partitions). Les documents abandonnés comptent en échec.
func (e *ES) Emit(docs []Doc) error {
	if err := e.EnsureSetup(context.Background()); err != nil {
		e.record(0, int64(len(docs)), err.Error())
		return err
	}
	var errs []error
	for start := 0; start < len(docs); start += e.opts.BulkMaxDocs {
		end := start + e.opts.BulkMaxDocs
		if end > len(docs) {
			end = len(docs)
		}
		err := e.bulk(docs[start:end])
		if err == nil {
			continue
		}
		errs = append(errs, err)
		if errors.Is(err, errUnreachable) && end < len(docs) {
			e.mu.Lock()
			e.failed += int64(len(docs) - end)
			e.mu.Unlock()
			errs = append(errs, fmt.Errorf("%d document(s) dropped without an attempt", len(docs)-end))
			break
		}
	}
	return errors.Join(errs...)
}

type bulkResponse struct {
	Errors bool `json:"errors"`
	Items  []map[string]struct {
		Status int `json:"status"`
		Error  *struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
	} `json:"items"`
}

func (e *ES) bulk(docs []Doc) error {
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, d := range docs {
		if err := enc.Encode(map[string]any{"create": map[string]any{"_index": e.Stream(d.Stream)}}); err != nil {
			return err
		}
		if err := enc.Encode(d.Body); err != nil {
			return err
		}
	}
	resp, err := e.do(context.Background(), http.MethodPost, "/_bulk", "application/x-ndjson", body.Bytes())
	if err != nil {
		e.record(0, int64(len(docs)), err.Error())
		return fmt.Errorf("bulk: %w", err)
	}
	var br bulkResponse
	if err := json.Unmarshal(resp, &br); err != nil {
		e.record(0, int64(len(docs)), err.Error())
		return fmt.Errorf("bulk: unreadable response: %w", err)
	}
	if !br.Errors {
		e.record(int64(len(docs)), 0, "")
		return nil
	}
	var failed, dups int64
	first := ""
	for _, item := range br.Items {
		for _, r := range item {
			switch {
			case r.Status == http.StatusConflict:
				// TSDS : même série au même @timestamp, déjà écrite (redémarrage
				// dans le même créneau de 10 s) ; compté à part, sans erreur
				dups++
			case r.Error != nil || r.Status >= 300:
				failed++
				if first == "" && r.Error != nil {
					first = r.Error.Type + ": " + r.Error.Reason
				}
			}
		}
	}
	e.record(int64(len(docs))-failed-dups, failed, first)
	e.mu.Lock()
	e.duplicates += dups
	e.mu.Unlock()
	if failed == 0 {
		return nil
	}
	return fmt.Errorf("bulk: %d document(s) rejected, first: %s", failed, first)
}

func (e *ES) record(sent, failed int64, lastErr string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.requests++
	e.sent += sent
	e.failed += failed
	if lastErr != "" {
		e.lastError = lastErr
	}
}

// Stats expose les compteurs pour le document de santé.
func (e *ES) Stats() map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	return map[string]any{"requests": e.requests, "sent": e.sent, "failed": e.failed, "duplicates": e.duplicates, "last_error": e.lastError}
}

// do envoie une requête, en essayant chaque URL une fois à partir de la
// courante. Une erreur de transport ou une réponse 502, 503 ou 504 (nœud ou
// mandataire indisponible) fait passer à l'URL suivante ; toute autre réponse
// HTTP, même en erreur, arrête les essais. Si aucune URL n'a répondu, l'erreur
// enveloppe errUnreachable.
func (e *ES) do(ctx context.Context, method, path, contentType string, body []byte) ([]byte, error) {
	e.mu.Lock()
	start := e.next
	e.mu.Unlock()
	var lastErr error
	for i := 0; i < len(e.opts.URLs); i++ {
		idx := (start + i) % len(e.opts.URLs)
		req, err := http.NewRequestWithContext(ctx, method, e.opts.URLs[idx]+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		switch {
		case e.opts.APIKey != "":
			req.Header.Set("Authorization", "ApiKey "+e.opts.APIKey)
		case e.opts.Username != "":
			req.SetBasicAuth(e.opts.Username, e.opts.Password)
		}
		resp, err := e.client.Do(req)
		if err != nil {
			lastErr = err
			e.mu.Lock()
			e.next = (idx + 1) % len(e.opts.URLs)
			e.mu.Unlock()
			continue
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		switch resp.StatusCode {
		case http.StatusNotFound:
			return data, errNotFound
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			lastErr = &httpError{Status: resp.StatusCode, Body: truncate(string(data), 300)}
			e.mu.Lock()
			e.next = (idx + 1) % len(e.opts.URLs)
			e.mu.Unlock()
			continue
		}
		if resp.StatusCode >= 300 {
			return data, &httpError{Status: resp.StatusCode, Body: truncate(string(data), 300)}
		}
		return data, nil
	}
	return nil, fmt.Errorf("%w: %v", errUnreachable, lastErr)
}

// maxResponseBytes borne la lecture d'une réponse (une réponse bulk de 2 000
// documents refusés reste bien en deçà).
const maxResponseBytes = 16 << 20

var (
	errNotFound    = errors.New("not found")
	errUnreachable = errors.New("no Elasticsearch URL responds")
)

type httpError struct {
	Status int
	Body   string
}

func (h *httpError) Error() string { return fmt.Sprintf("HTTP %d: %s", h.Status, h.Body) }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// Setup installe les politiques ILM et les index templates des quatre data
// streams, sans écraser une politique ILM déjà présente (elle appartient à
// l'exploitant) ; les templates sont remis à jour si leur version est plus
// ancienne, et le data stream existant basculé sur un nouvel index : sinon un
// champ ajouté au mapping reste inconnu d'ES|QL (colonne inconnue) jusqu'au
// rollover suivant. Les flux de lag et de métriques passent ainsi en TSDS ;
// leurs anciens index classiques gardent la politique <prefix>, qui ne les
// sous-échantillonne pas.
func (e *ES) Setup(ctx context.Context) error {
	policy, tsPolicy := e.opts.Prefix, e.opts.Prefix+"-timeseries"
	for name, body := range map[string]map[string]any{policy: ilmPolicy(), tsPolicy: timeSeriesPolicy()} {
		if _, err := e.do(ctx, http.MethodGet, "/_ilm/policy/"+name, "", nil); errors.Is(err, errNotFound) {
			if _, err := e.do(ctx, http.MethodPut, "/_ilm/policy/"+name, "application/json", mustJSON(body)); err != nil {
				return fmt.Errorf("ILM policy %s: %w", name, err)
			}
			e.logger.Printf("ILM policy %s created", name)
		} else if err != nil {
			return fmt.Errorf("ILM policy %s: %w", name, err)
		}
	}
	streams := []struct {
		stream, policy string
		mapping        map[string]any
		dimensions     []string
	}{
		{"lag", tsPolicy, lagMapping(), lagDimensions},
		{"metrics", tsPolicy, metricsMapping(), metricsDimensions},
		{"health", policy, healthMapping(), nil},
		{"events", policy, eventsMapping(), nil},
	}
	for _, s := range streams {
		name := e.Stream(s.stream)
		if !e.upToDate(ctx, "/_index_template/"+name) {
			tpl := indexTemplate(name, s.policy, s.mapping, s.dimensions)
			if _, err := e.do(ctx, http.MethodPut, "/_index_template/"+name, "application/json", mustJSON(tpl)); err != nil {
				return fmt.Errorf("index template %s: %w", name, err)
			}
			e.logger.Printf("index template %s installed (version %d)", name, templateVersion)
			if _, err := e.do(ctx, http.MethodPost, "/"+name+"/_rollover", "", nil); err == nil {
				e.logger.Printf("data stream %s rolled over to the new mapping", name)
			} else if !errors.Is(err, errNotFound) {
				return fmt.Errorf("rollover %s: %w", name, err)
			}
		}
		// data stream créé d'avance : un dashboard qui le lit (rebalances, aucun
		// encore survenu) ne doit pas échouer sur un index inconnu
		if _, err := e.do(ctx, http.MethodGet, "/_data_stream/"+name, "", nil); errors.Is(err, errNotFound) {
			if _, err := e.do(ctx, http.MethodPut, "/_data_stream/"+name, "", nil); err != nil {
				return fmt.Errorf("data stream %s: %w", name, err)
			}
		} else if err != nil {
			return fmt.Errorf("data stream %s: %w", name, err)
		}
	}
	return nil
}

// upToDate lit la version du template installé.
func (e *ES) upToDate(ctx context.Context, path string) bool {
	data, err := e.do(ctx, http.MethodGet, path, "", nil)
	if err != nil {
		return false
	}
	var got struct {
		Templates []struct {
			IndexTemplate struct {
				Version int `json:"version"`
			} `json:"index_template"`
		} `json:"index_templates"`
	}
	if json.Unmarshal(data, &got) != nil || len(got.Templates) == 0 {
		return false
	}
	return got.Templates[0].IndexTemplate.Version >= templateVersion
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
