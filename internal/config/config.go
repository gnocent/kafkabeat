// Package config charge, complète et valide la configuration YAML de kafkabeat.
package config

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config est la configuration complète de l'agent.
type Config struct {
	Cluster string  `yaml:"cluster"`
	Kafka   Kafka   `yaml:"kafka"`
	Collect Collect `yaml:"collect"`
	History History `yaml:"history"`
	State   State   `yaml:"state"`
	Output  Output  `yaml:"output"`
	Jolokia Jolokia `yaml:"jolokia"`
}

// Jolokia : metricset broker (kafka.broker.*, noms Metricbeat), lu à
// l'intervalle des métriques sur l'agent Jolokia de chaque broker découvert
// par les métadonnées (hôte du broker, port et chemin de la configuration).
type Jolokia struct {
	Enabled  bool          `yaml:"enabled"`
	Port     int           `yaml:"port"`
	Path     string        `yaml:"path"`
	Username string        `yaml:"username"`
	Password string        `yaml:"password"`
	TLS      TLS           `yaml:"tls"`
	Timeout  time.Duration `yaml:"timeout"`
}

// Kafka décrit la connexion au cluster supervisé.
type Kafka struct {
	Brokers  []string `yaml:"brokers"`
	ClientID string   `yaml:"client_id"`
	TLS      TLS      `yaml:"tls"`
	SASL     SASL     `yaml:"sasl"`
}

// TLS est commun à Kafka et Elasticsearch.
type TLS struct {
	Enabled  bool   `yaml:"enabled"`
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// SASL : mécanisme vide = pas d'authentification.
type SASL struct {
	Mechanism string `yaml:"mechanism"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

// Collect règle la collecte et l'émission.
type Collect struct {
	Interval        time.Duration `yaml:"interval"`
	MetricsInterval time.Duration `yaml:"metrics_interval"`
	Topics          Filter        `yaml:"topics"`
	Groups          Filter        `yaml:"groups"`
	StalledAfter    time.Duration `yaml:"stalled_after"`
	FlagHoldCycles  int           `yaml:"flag_hold_cycles"`
	GoneCycles      int           `yaml:"gone_cycles"`
	SmoothingWindow time.Duration `yaml:"smoothing_window"`
	Commits         Commits       `yaml:"commits"`
}

// Commits choisit la source des offsets commités.
type Commits struct {
	Mode              string        `yaml:"mode"`
	ReconcileInterval time.Duration `yaml:"reconcile_interval"`
}

// Filter : liste blanche puis liste noire, motifs glob (path.Match).
type Filter struct {
	Include []string `yaml:"include"`
	Exclude []string `yaml:"exclude"`
}

// Match indique si name passe le filtre : aucun include ou un include qui
// correspond, et aucun exclude qui corresponde.
func (f Filter) Match(name string) bool {
	if len(f.Include) > 0 && !matchAny(f.Include, name) {
		return false
	}
	return !matchAny(f.Exclude, name)
}

func matchAny(patterns []string, name string) bool {
	for _, p := range patterns {
		if ok, _ := path.Match(p, name); ok {
			return true
		}
	}
	return false
}

// History décrit les paliers de granularité de l'historique des offsets de fin.
type History struct {
	Tiers []Tier `yaml:"tiers"`
}

// Tier : un échantillon conservé au plus tous les Step pendant Span.
type Tier struct {
	Step time.Duration `yaml:"step"`
	Span time.Duration `yaml:"span"`
}

// State : persistance sur disque.
type State struct {
	Path             string        `yaml:"path"`
	SnapshotInterval time.Duration `yaml:"snapshot_interval"`
}

// Output : sorties et enrichissement statique.
type Output struct {
	Elasticsearch Elasticsearch     `yaml:"elasticsearch"`
	Fields        map[string]string `yaml:"fields"`
	Owners        []Owner           `yaml:"owners"`
}

// Owner rattache les topics dont le nom correspond au motif glob à un
// propriétaire (kafka.topic.owner). Liste ordonnée : la première règle qui
// correspond l'emporte.
type Owner struct {
	Topic string `yaml:"topic"`
	Owner string `yaml:"owner"`
}

// Elasticsearch : connexion et data streams.
type Elasticsearch struct {
	URLs        []string `yaml:"urls"`
	Username    string   `yaml:"username"`
	Password    string   `yaml:"password"`
	APIKey      string   `yaml:"api_key"`
	TLS         TLS      `yaml:"tls"`
	Setup       bool     `yaml:"setup"`
	Prefix      string   `yaml:"prefix"`
	BulkMaxDocs int      `yaml:"bulk_max_docs"`
}

// Default renvoie la configuration par défaut, complète hormis cluster,
// brokers et urls.
func Default() Config {
	return Config{
		Kafka: Kafka{ClientID: "kafkabeat"},
		Collect: Collect{
			Interval:        10 * time.Second,
			MetricsInterval: 60 * time.Second,
			StalledAfter:    60 * time.Second,
			FlagHoldCycles:  6,
			GoneCycles:      6,
			SmoothingWindow: 60 * time.Second,
			Commits:         Commits{Mode: "hybrid", ReconcileInterval: 2 * time.Minute},
		},
		History: History{Tiers: []Tier{
			{Step: 10 * time.Second, Span: 15 * time.Minute},
			{Step: time.Minute, Span: 3 * time.Hour},
			{Step: 10 * time.Minute, Span: 7 * 24 * time.Hour},
		}},
		State:   State{Path: "./state", SnapshotInterval: 5 * time.Minute},
		Output:  Output{Elasticsearch: Elasticsearch{Setup: true, Prefix: "kafkabeat", BulkMaxDocs: 2000}},
		Jolokia: Jolokia{Port: 8778, Path: "/jolokia/", Timeout: 10 * time.Second},
	}
}

// Load lit le fichier YAML par-dessus les valeurs par défaut et valide.
func Load(filename string) (Config, error) {
	cfg := Default()
	raw, err := os.ReadFile(filename)
	if err != nil {
		return cfg, err
	}
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("%s: %w", filename, err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("%s: %w", filename, err)
	}
	return cfg, nil
}

var saslMechanisms = map[string]bool{"": true, "PLAIN": true, "SCRAM-SHA-256": true, "SCRAM-SHA-512": true}

// reservedFields : clés de premier niveau des documents, que l'enrichissement
// statique ne doit pas remplacer (_stream est celle de la sortie standard).
var reservedFields = map[string]bool{"@timestamp": true, "event": true, "kafka": true, "kafkabeat": true, "metricset": true, "service": true, "_stream": true}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Validate vérifie la cohérence de la configuration.
func (c Config) Validate() error {
	var errs []error
	if c.Cluster == "" {
		errs = append(errs, errors.New("cluster is required"))
	}
	if len(c.Kafka.Brokers) == 0 {
		errs = append(errs, errors.New("kafka.brokers is required"))
	}
	if !saslMechanisms[c.Kafka.SASL.Mechanism] {
		errs = append(errs, fmt.Errorf("unknown kafka.sasl.mechanism: %q", c.Kafka.SASL.Mechanism))
	}
	if c.Collect.Interval <= 0 || c.Collect.MetricsInterval <= 0 {
		errs = append(errs, errors.New("collect.interval and collect.metrics_interval must be positive"))
	}
	if c.Collect.StalledAfter <= 0 || c.Collect.SmoothingWindow <= 0 {
		errs = append(errs, errors.New("collect.stalled_after and collect.smoothing_window must be positive"))
	}
	// à 0, une paire disparue serait émise gone sans fin et jamais purgée, et aucun flag ne serait levé
	if c.Collect.FlagHoldCycles < 1 || c.Collect.GoneCycles < 1 {
		errs = append(errs, errors.New("collect.flag_hold_cycles and collect.gone_cycles must be at least 1"))
	}
	if c.Collect.Commits.Mode != "polling" && c.Collect.Commits.Mode != "hybrid" {
		errs = append(errs, fmt.Errorf("collect.commits.mode must be polling or hybrid, not %q", c.Collect.Commits.Mode))
	}
	if c.Collect.Commits.ReconcileInterval <= 0 {
		errs = append(errs, errors.New("collect.commits.reconcile_interval must be positive"))
	}
	if len(c.History.Tiers) == 0 {
		errs = append(errs, errors.New("history.tiers is required"))
	}
	for i, t := range c.History.Tiers {
		if t.Step <= 0 || t.Span <= 0 {
			errs = append(errs, fmt.Errorf("history.tiers[%d]: step and span must be positive", i))
		}
		if i > 0 && (t.Step <= c.History.Tiers[i-1].Step || t.Span <= c.History.Tiers[i-1].Span) {
			errs = append(errs, fmt.Errorf("history.tiers[%d]: step and span must increase from one tier to the next", i))
		}
	}
	if c.State.Path == "" {
		errs = append(errs, errors.New("state.path is required"))
	}
	if c.State.SnapshotInterval <= 0 {
		errs = append(errs, errors.New("state.snapshot_interval must be positive"))
	}
	if len(c.Output.Elasticsearch.URLs) == 0 {
		errs = append(errs, errors.New("output.elasticsearch.urls is required"))
	}
	if c.Output.Elasticsearch.Prefix == "" {
		errs = append(errs, errors.New("output.elasticsearch.prefix is required"))
	}
	if c.Jolokia.Enabled {
		if c.Jolokia.Port <= 0 || c.Jolokia.Port > 65535 {
			errs = append(errs, fmt.Errorf("invalid jolokia.port: %d", c.Jolokia.Port))
		}
		if c.Jolokia.Timeout <= 0 || c.Jolokia.Timeout >= c.Collect.MetricsInterval {
			errs = append(errs, errors.New("jolokia.timeout must be positive and less than collect.metrics_interval"))
		}
	}
	for _, k := range sortedKeys(c.Output.Fields) {
		if reservedFields[k] {
			errs = append(errs, fmt.Errorf("output.fields: reserved key %q (it would overwrite the document)", k))
		}
	}
	owners := make([]string, len(c.Output.Owners))
	for i, o := range c.Output.Owners {
		owners[i] = o.Topic
		if o.Topic == "" || o.Owner == "" {
			errs = append(errs, fmt.Errorf("output.owners[%d]: topic and owner are required", i))
		}
	}
	for _, pats := range [][]string{c.Collect.Topics.Include, c.Collect.Topics.Exclude, c.Collect.Groups.Include, c.Collect.Groups.Exclude, owners} {
		for _, p := range pats {
			if _, err := path.Match(p, ""); err != nil {
				errs = append(errs, fmt.Errorf("invalid pattern %q", p))
			}
		}
	}
	return errors.Join(errs...)
}

// TopicOwner renvoie le propriétaire de la première règle dont le motif
// correspond au topic, ou "" s'il n'y en a aucune.
func (c Config) TopicOwner(topic string) string {
	for _, o := range c.Output.Owners {
		if ok, _ := path.Match(o.Topic, topic); ok {
			return o.Owner
		}
	}
	return ""
}

// TopicSelected applique le filtre des topics.
func (c Config) TopicSelected(topic string) bool {
	return c.Collect.Topics.Match(topic)
}

// GroupSelected applique le filtre des groupes.
func (c Config) GroupSelected(group string) bool {
	return c.Collect.Groups.Match(group)
}
