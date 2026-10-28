// kafkabeat mesure le lag temporel Kafka par topic et consumer group et le
// publie dans Elasticsearch. Voir DESIGN.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gnocent/kafkabeat/internal/collector"
	"github.com/gnocent/kafkabeat/internal/config"
	"github.com/gnocent/kafkabeat/internal/history"
	"github.com/gnocent/kafkabeat/internal/jolokia"
	"github.com/gnocent/kafkabeat/internal/kafkaclient"
	"github.com/gnocent/kafkabeat/internal/offsetstream"
	"github.com/gnocent/kafkabeat/internal/sink"
)

var version = "dev"

func main() {
	cfgPath := flag.String("config", "kafkabeat.yml", "path to the configuration file")
	showVersion := flag.Bool("version", false, "print the version and exit")
	once := flag.Bool("once", false, "run a single cycle and exit")
	stdout := flag.Bool("stdout", false, "write documents to standard output instead of Elasticsearch")
	flag.Parse()
	if *showVersion {
		fmt.Println("kafkabeat", version)
		return
	}
	logger := log.New(os.Stderr, "", log.LstdFlags|log.LUTC)
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		logger.Printf("configuration: %v", err)
		os.Exit(2)
	}
	if err := run(cfg, logger, *once, *stdout); err != nil {
		logger.Printf("stopping: %v", err)
		os.Exit(1)
	}
}

func run(cfg config.Config, logger *log.Logger, once, toStdout bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	kc, err := kafkaclient.New(cfg.Kafka)
	if err != nil {
		return fmt.Errorf("Kafka client: %w", err)
	}
	defer kc.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := kc.Ping(pingCtx); err != nil {
		return fmt.Errorf("Kafka unreachable: %w", err)
	}

	logger.Printf("kafkabeat %s: cluster %s, %d broker(s), cycle %s, commits %s, jolokia %v",
		version, cfg.Cluster, len(cfg.Kafka.Brokers), cfg.Collect.Interval, cfg.Collect.Commits.Mode, cfg.Jolokia.Enabled)
	// flux __consumer_offsets ouvert avant tout le reste : son consommateur
	// part de la fin du topic, chaque instant passé avant qu'il soit en place
	// est un enregistrement de groupe (génération) qui peut lui échapper
	var commits collector.CommitSource = kc
	if cfg.Collect.Commits.Mode == "hybrid" {
		opts, err := kafkaclient.Options(cfg.Kafka)
		if err != nil {
			return err
		}
		stream, err := offsetstream.New(ctx, opts, kc, cfg.Collect.Commits.ReconcileInterval, logger)
		if err != nil {
			return err
		}
		defer stream.Close()
		commits = stream
	}

	var out sink.Sink
	if toStdout {
		out = sink.NewStdout(os.Stdout)
	} else {
		es, err := newES(cfg, logger)
		if err != nil {
			return err
		}
		// templates et politiques installés avant le premier cycle : une
		// installation longue retarderait le cycle d'un créneau entier
		setupCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		if err := es.EnsureSetup(setupCtx); err != nil {
			logger.Printf("%v: will retry on first send", err)
		}
		cancel()
		out = es
	}
	col := collector.New(cfg, kc, commits, history.New(collector.HistoryTiers(cfg.History)), out, logger, version)
	if cfg.Jolokia.Enabled {
		jc := cfg.Jolokia
		opts := jolokia.Options{Port: jc.Port, Path: jc.Path, Username: jc.Username, Password: jc.Password, Timeout: jc.Timeout,
			Period: cfg.Collect.MetricsInterval, Cluster: cfg.Cluster, Fields: cfg.Output.Fields, Owner: cfg.TopicOwner}
		if jc.TLS.Enabled {
			tc, err := kafkaclient.TLSConfig(jc.TLS)
			if err != nil {
				return fmt.Errorf("jolokia TLS: %w", err)
			}
			opts.TLS = tc
		}
		// metricset broker : boucle propre, hors du cycle de 10 s
		jp := jolokia.New(opts, kc.Brokers, out, logger)
		col.SetJolokia(jp)
		if once {
			jp.Poll(ctx, time.Now())
		} else {
			go jp.Run(ctx)
		}
	}
	if savedAt, err := col.Load(cfg.State.Path, time.Now()); err != nil {
		logger.Printf("persisted state ignored: %v", err)
	} else if !savedAt.IsZero() {
		logger.Printf("state restored from %s (saved at %s)", cfg.State.Path, savedAt.UTC().Format(time.RFC3339))
	}
	if once {
		col.Cycle(ctx, time.Now())
		return nil
	}
	if err := col.Run(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	logger.Printf("shutdown requested")
	return nil
}

func newES(cfg config.Config, logger *log.Logger) (*sink.ES, error) {
	ec := cfg.Output.Elasticsearch
	opts := sink.ESOptions{
		URLs: ec.URLs, Username: ec.Username, Password: ec.Password, APIKey: ec.APIKey,
		Prefix: ec.Prefix, BulkMaxDocs: ec.BulkMaxDocs,
	}
	if ec.TLS.Enabled {
		tc, err := kafkaclient.TLSConfig(ec.TLS)
		if err != nil {
			return nil, fmt.Errorf("elasticsearch TLS: %w", err)
		}
		opts.TLS = tc
	}
	es, err := sink.NewES(opts, logger)
	if err != nil {
		return nil, err
	}
	if ec.Setup {
		es.WantSetup()
	}
	return es, nil
}
