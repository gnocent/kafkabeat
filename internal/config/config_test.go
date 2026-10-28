package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadExample(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "kafkabeat.example.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster != "bench-39" || cfg.Collect.Interval != 10*time.Second || len(cfg.History.Tiers) != 3 {
		t.Fatalf("configuration inattendue : %+v", cfg)
	}
}

func TestDefaultsAndValidation(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "k.yml")
	if err := os.WriteFile(file, []byte("cluster: c\nkafka:\n  brokers: [b:9092]\noutput:\n  elasticsearch:\n    urls: [http://es:9200]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(file)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Collect.Commits.Mode != "hybrid" || cfg.State.SnapshotInterval != 5*time.Minute || cfg.Output.Elasticsearch.Prefix != "kafkabeat" ||
		cfg.Jolokia.Enabled || cfg.Jolokia.Port != 8778 || cfg.Jolokia.Timeout != 10*time.Second {
		t.Fatalf("défauts non appliqués : %+v", cfg)
	}
	cfg.Jolokia.Enabled, cfg.Jolokia.Timeout = true, 2*time.Minute
	if err := cfg.Validate(); err == nil {
		t.Fatal("un délai Jolokia supérieur à l'intervalle des métriques doit être refusé")
	}
	if err := os.WriteFile(file, []byte("cluster: c\nkafka:\n  brokers: [b:9092]\n  sasl: {mechanism: GSSAPI}\ncollect:\n  commits: {mode: stream}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(file); err == nil {
		t.Fatal("une configuration invalide doit être refusée")
	}
	if err := os.WriteFile(file, []byte("cluster: c\nunknown: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(file); err == nil {
		t.Fatal("une clé inconnue doit être refusée")
	}
}

func TestValidateDurationsAndCycles(t *testing.T) {
	valid := func() Config {
		cfg := Default()
		cfg.Cluster, cfg.Kafka.Brokers, cfg.Output.Elasticsearch.URLs = "c", []string{"b:9092"}, []string{"http://es:9200"}
		return cfg
	}
	if err := valid().Validate(); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*Config){
		"stalled_after":     func(c *Config) { c.Collect.StalledAfter = 0 },
		"smoothing_window":  func(c *Config) { c.Collect.SmoothingWindow = -time.Second },
		"flag_hold_cycles":  func(c *Config) { c.Collect.FlagHoldCycles = 0 },
		"gone_cycles":       func(c *Config) { c.Collect.GoneCycles = 0 },
		"snapshot_interval": func(c *Config) { c.State.SnapshotInterval = 0 },
	}
	for name, breakIt := range cases {
		cfg := valid()
		breakIt(&cfg)
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s nul ou négatif doit être refusé en le nommant, reçu %v", name, err)
		}
	}
}

func TestReservedOutputFields(t *testing.T) {
	cfg := Default()
	cfg.Cluster, cfg.Kafka.Brokers, cfg.Output.Elasticsearch.URLs = "c", []string{"b:9092"}, []string{"http://es:9200"}
	cfg.Output.Fields = map[string]string{"site": "lab", "env": "prod"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Output.Fields = map[string]string{"site": "lab", "kafka": "x", "@timestamp": "y"}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), `reserved key "kafka"`) || !strings.Contains(err.Error(), `reserved key "@timestamp"`) {
		t.Fatalf("une clé de document dans output.fields doit être refusée en la nommant, reçu %v", err)
	}
}

func TestFilters(t *testing.T) {
	cfg := Default()
	cfg.Collect.Topics = Filter{Include: []string{"logs-*"}, Exclude: []string{"logs-debug*"}}
	cases := map[string]bool{"logs-app": true, "logs-debug-1": false, "metrics": false, "__consumer_offsets": false}
	for name, want := range cases {
		if got := cfg.TopicSelected(name); got != want {
			t.Errorf("TopicSelected(%q) = %v, attendu %v", name, got, want)
		}
	}
	if !cfg.GroupSelected("anything") {
		t.Error("sans filtre, tout groupe doit être retenu")
	}
	if !Default().TopicSelected("__consumer_offsets") {
		t.Error("sans filtre, les topics internes sont retenus (documents partition, comme Metricbeat)")
	}
}

func TestTopicOwner(t *testing.T) {
	cfg := Default()
	cfg.Cluster, cfg.Kafka.Brokers, cfg.Output.Elasticsearch.URLs = "c", []string{"b:9092"}, []string{"http://es:9200"}
	cfg.Output.Owners = []Owner{{Topic: "pay-*", Owner: "paiement"}, {Topic: "pay-*", Owner: "jamais"}, {Topic: "*-logs", Owner: "logs"}}
	cases := map[string]string{"pay-api-logs": "paiement", "web-logs": "logs", "metrics": ""}
	for name, want := range cases {
		if got := cfg.TopicOwner(name); got != want {
			t.Errorf("TopicOwner(%q) = %q, attendu %q", name, got, want)
		}
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Output.Owners = []Owner{{Topic: "[", Owner: "x"}, {Topic: "a", Owner: ""}}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "invalid pattern") || !strings.Contains(err.Error(), "owners[1]") {
		t.Fatalf("motif invalide et propriétaire vide doivent être refusés, reçu %v", err)
	}
}
