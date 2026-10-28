package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// runTopics traite `harness topics create|delete|recreate`.
func runTopics(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: harness topics create|delete|recreate [options]")
	}
	sub := args[0]
	fs, bootstrap := newFlagSet("topics " + sub)
	name := fs.String("name", "", "topic name")
	partitions := fs.Int("partitions", 1, "number of partitions (create, recreate)")
	configList := fs.String("config", "", "topic configuration (create, recreate), e.g. retention.ms=20000,segment.ms=2000")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	configs := map[string]*string{}
	for _, kv := range splitList(*configList) {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return fmt.Errorf("--config: expected key=value, got %q", kv)
		}
		configs[k] = &v
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	if *partitions < 1 {
		return errors.New("--partitions must be >= 1")
	}

	cl, err := newClient(ctx, *bootstrap)
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	switch sub {
	case "create":
		return createTopic(ctx, adm, *name, int32(*partitions), configs)
	case "delete":
		return deleteTopic(ctx, adm, *name)
	case "recreate":
		if err := deleteTopic(ctx, adm, *name); err != nil {
			return err
		}
		if err := waitTopicGone(ctx, adm, *name, 30*time.Second); err != nil {
			return err
		}
		return createTopic(ctx, adm, *name, int32(*partitions), configs)
	default:
		return fmt.Errorf("unknown topics subcommand: %s", sub)
	}
}

// createTopic crée le topic (facteur de réplication 1) ; réessaie tant que le broker
// signale un topic encore en cours de suppression.
func createTopic(ctx context.Context, adm *kadm.Client, name string, partitions int32, configs map[string]*string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		resps, err := adm.CreateTopics(ctx, partitions, 1, configs, name)
		if err == nil {
			r, ok := resps[name]
			if !ok {
				return fmt.Errorf("creating %s: no response", name)
			}
			err = r.Err
			if err == nil {
				logger.Printf("topic %s created: %d partitions, id %s", name, partitions, r.ID)
				return nil
			}
		}
		if errors.Is(err, kerr.TopicAlreadyExists) && time.Now().Before(deadline) {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return fmt.Errorf("creating %s: %w", name, err)
	}
}

func deleteTopic(ctx context.Context, adm *kadm.Client, name string) error {
	resps, err := adm.DeleteTopics(ctx, name)
	if err != nil {
		return fmt.Errorf("deleting %s: %w", name, err)
	}
	r, ok := resps[name]
	if !ok {
		return fmt.Errorf("deleting %s: no response", name)
	}
	if r.Err != nil {
		if errors.Is(r.Err, kerr.UnknownTopicOrPartition) {
			logger.Printf("topic %s missing, nothing to delete", name)
			return nil
		}
		return fmt.Errorf("deleting %s: %w", name, r.Err)
	}
	logger.Printf("topic %s deleted (id %s)", name, r.ID)
	return nil
}

// waitTopicGone attend que le topic disparaisse des métadonnées.
func waitTopicGone(ctx context.Context, adm *kadm.Client, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		details, err := adm.ListTopics(ctx, name)
		if err != nil {
			return fmt.Errorf("metadata of %s: %w", name, err)
		}
		if !details.Has(name) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("topic %s still exists after %s", name, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// waitTopics attend que les métadonnées décrivent les topics, sans erreur :
// juste après sa création, un topic n'est pas encore connu de tous (le
// producteur ou le consumer lancé aussitôt s'arrêtait sur UNKNOWN_TOPIC_OR_PARTITION).
func waitTopics(ctx context.Context, adm *kadm.Client, topics []string) (kadm.TopicDetails, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		details, err := adm.ListTopics(ctx, topics...)
		if err == nil {
			err = details.Error()
		}
		if err == nil && len(details) == len(topics) {
			return details, nil
		}
		if err == nil {
			err = fmt.Errorf("topics %v not found", topics)
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("metadata: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}
