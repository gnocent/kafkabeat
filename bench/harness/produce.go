package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gnocent/kafkabeat/bench/harness/journal"
)

// phase est un palier de production : débit constant pendant une durée.
type phase struct {
	Name     string
	Rate     float64 // messages par seconde, 0 = silence
	Duration time.Duration
}

// parsePhases lit un profil `nom:débit/s:durée,...`, ex. `steady:100:60s,silence:0:120s,burst:5000:10s`.
func parsePhases(spec string) ([]phase, error) {
	var phases []phase
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.Split(item, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("phase %q: expected name:rate:duration", item)
		}
		rate, err := strconv.ParseFloat(parts[1], 64)
		if err != nil || rate < 0 {
			return nil, fmt.Errorf("phase %q: invalid rate", item)
		}
		d, err := time.ParseDuration(parts[2])
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("phase %q: invalid duration", item)
		}
		phases = append(phases, phase{Name: parts[0], Rate: rate, Duration: d})
	}
	if len(phases) == 0 {
		return nil, errors.New("--phases is empty")
	}
	return phases, nil
}

// runProduce traite `harness produce`.
func runProduce(ctx context.Context, args []string) error {
	fs, bootstrap := newFlagSet("produce")
	topic := fs.String("topic", "", "target topic")
	phasesSpec := fs.String("phases", "", "production profile name:rate/s:duration,... (e.g. steady:100:60s,silence:0:120s,burst:5000:10s)")
	repeat := fs.Bool("repeat", false, "replay the phases in a loop until interrupted")
	size := fs.Int("size", 100, "size of each message value in bytes")
	journalPath := fs.String("journal", "", "journal file (one JSON line per produced message)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" || *phasesSpec == "" || *journalPath == "" {
		return errors.New("--topic, --phases and --journal are required")
	}
	if *size < 1 {
		return errors.New("--size must be >= 1")
	}
	phases, err := parsePhases(*phasesSpec)
	if err != nil {
		return err
	}

	cl, err := newClient(ctx, *bootstrap,
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.MaxBufferedRecords(50_000),
		kgo.RecordRetries(10),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	// Le topic doit exister (auto-création désactivée sur le banc).
	details, err := waitTopics(ctx, kadm.NewClient(cl), []string{*topic})
	if err != nil {
		return fmt.Errorf("%w: create it with `harness topics create`", err)
	}
	td := details[*topic]
	logger.Printf("produce: topic %s (%d partitions), %d phase(s), repeat=%v, journal %s",
		*topic, len(td.Partitions), len(phases), *repeat, *journalPath)

	jw, err := journal.NewWriter(*journalPath)
	if err != nil {
		return err
	}
	// Flush régulier du journal (les callbacks n'écrivent que dans le tampon).
	flushDone := make(chan struct{})
	var flushWG sync.WaitGroup
	flushWG.Add(1)
	go func() {
		defer flushWG.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-flushDone:
				return
			case <-t.C:
				if err := jw.Flush(); err != nil {
					logger.Printf("journal: flush: %v", err)
				}
			}
		}
	}()

	p := &producer{cl: cl, topic: *topic, value: make([]byte, *size), jw: jw}
	if _, err := rand.Read(p.value); err != nil {
		return err
	}

	start := time.Now()
	loop := 0
	for {
		loop++
		for _, ph := range phases {
			if ctx.Err() != nil {
				break
			}
			logger.Printf("phase %s: %.0f msg/s for %s (loop %d)", ph.Name, ph.Rate, ph.Duration, loop)
			p.runPhase(ctx, ph)
		}
		if !*repeat || ctx.Err() != nil {
			break
		}
	}

	// Attend les accusés de réception en suspens (contexte neuf : le nôtre est peut-être annulé).
	flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.Flush(flushCtx); err != nil {
		logger.Printf("producer flush: %v", err)
	}
	close(flushDone)
	flushWG.Wait()
	if err := jw.Close(); err != nil {
		return fmt.Errorf("closing journal: %w", err)
	}

	sent, acked, failed := p.sent.Load(), p.acked.Load(), p.failed.Load()
	logger.Printf("produce done in %s: %d sent, %d acked (journaled), %d failed",
		time.Since(start).Round(time.Millisecond), sent, acked, failed)
	if failed > 0 {
		return fmt.Errorf("%d message(s) failed", failed)
	}
	return nil
}

type producer struct {
	cl    *kgo.Client
	topic string
	value []byte
	jw    *journal.Writer

	sent, acked, failed atomic.Int64
	errLogged           atomic.Int32
}

// runPhase produit au débit de la phase jusqu'à la fin de sa durée, en rattrapant
// le retard accumulé tick par tick (pas de dérive sur les longues phases).
func (p *producer) runPhase(ctx context.Context, ph phase) {
	end := time.Now().Add(ph.Duration)
	if ph.Rate == 0 {
		select {
		case <-ctx.Done():
		case <-time.After(ph.Duration):
		}
		return
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	last := time.Now()
	owed := 0.0
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			if now.After(end) {
				return
			}
			owed += ph.Rate * now.Sub(last).Seconds()
			last = now
			n := int(owed)
			owed -= float64(n)
			for i := 0; i < n; i++ {
				p.produceOne(ctx)
			}
		}
	}
}

// produceOne envoie un message à clé aléatoire et journalise son offset à l'acquittement.
func (p *producer) produceOne(ctx context.Context) {
	var kb [8]byte
	_, _ = rand.Read(kb[:])
	sentAt := time.Now()
	rec := &kgo.Record{
		Topic:     p.topic,
		Key:       []byte(hex.EncodeToString(kb[:])),
		Value:     p.value,
		Timestamp: sentAt,
	}
	p.sent.Add(1)
	p.cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
		if err != nil {
			p.failed.Add(1)
			if p.errLogged.Add(1) <= 5 {
				logger.Printf("produce: %v", err)
			}
			return
		}
		p.acked.Add(1)
		if err := p.jw.Write(journal.Entry{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, ProducedAt: sentAt}); err != nil {
			if p.errLogged.Add(1) <= 5 {
				logger.Printf("journal: %v", err)
			}
		}
	})
}
