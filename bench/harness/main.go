// Harnais du banc de test kafkabeat : gestion des topics et groupes, producteur à
// débit par phases avec journal des heures de production, consumer pilotable par HTTP.
//
// Usage :
//
//	harness topics create   --bootstrap <hôte:port> --name <t> --partitions <n>
//	harness topics delete   --bootstrap <hôte:port> --name <t>
//	harness topics recreate --bootstrap <hôte:port> --name <t> --partitions <n>
//	harness groups delete   --bootstrap <hôte:port> --group <g>
//	harness produce --bootstrap <hôte:port> --topic <t> --phases "<nom:débit/s:durée,...>" [--repeat] [--size <octets>] --journal <fichier>
//	harness consume --bootstrap <hôte:port> --group <g> --topics <t1,t2> --commit auto|manual [--commit-interval 5s] [--max-rate <msgs/s>] --control <adresse>
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const usage = `usage: harness <command> [options]

commands:
  topics create|delete|recreate   manage topics (--name, --partitions)
  groups delete                   delete a consumer group (--group)
  produce                         produce in phases with a journal (--topic, --phases, --journal)
  consume                         consume, controlled over HTTP (--group, --topics, --commit, --control)

Every command accepts --bootstrap <host:port> (default localhost:19092) and -h for its options.
`

var logger = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "topics":
		err = runTopics(ctx, os.Args[2:])
	case "groups":
		err = runGroups(ctx, os.Args[2:])
	case "produce":
		err = runProduce(ctx, os.Args[2:])
	case "consume":
		err = runConsume(ctx, os.Args[2:])
	case "-h", "--help", "help":
		fmt.Fprint(os.Stderr, usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		if err == flag.ErrHelp {
			os.Exit(2)
		}
		logger.Printf("error: %v", err)
		os.Exit(1)
	}
}

// newFlagSet crée un jeu d'options avec l'option commune --bootstrap.
func newFlagSet(name string) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	bootstrap := fs.String("bootstrap", "localhost:19092", "Kafka brokers (host:port, comma-separated)")
	return fs, bootstrap
}

// newClient ouvre un client franz-go et vérifie la connexion aux brokers.
func newClient(ctx context.Context, bootstrap string, opts ...kgo.Opt) (*kgo.Client, error) {
	seeds := strings.Split(bootstrap, ",")
	base := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ClientID("kafkabeat-harness"),
		kgo.DialTimeout(5 * time.Second),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("Kafka client: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("connecting to %s: %w", bootstrap, err)
	}
	return cl, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
