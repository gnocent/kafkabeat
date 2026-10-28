// Package offsetstream lit le topic interne __consumer_offsets en flux et
// maintient la vue courante des offsets commités, réconciliée périodiquement
// par l'API publique (OffsetFetch) ; les groupes dont un enregistrement a une
// version de valeur inconnue sont basculés définitivement sur le polling.
//
// Clés : versions 0 et 1 = commit d'offset ; 2 = métadonnées de groupe
// classique (membres, abonnements, assignations, à chaque génération) ; toute
// autre version (nouveau coordinateur, KIP-848 et suivants) est ignorée et
// comptée. Valeurs connues : commit 0 à 4, métadonnées de groupe 0 à 4 ; un
// groupe dont les métadonnées sont en version inconnue est signalé en repli
// (ses membres sont alors lus par DescribeGroups).
package offsetstream

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/gnocent/kafkabeat/internal/kafkaclient"
)

// Topic est le topic interne des offsets.
const Topic = "__consumer_offsets"

const (
	keyVersionCommitMax   = 1
	keyVersionGroupMeta   = 2
	valueVersionCommitMax = 4
	valueVersionGroupMax  = 4
)

// Poller est la source de secours et de réconciliation (API publique).
type Poller interface {
	Commits(ctx context.Context, groups []string) (kafkaclient.GroupCommits, map[string]error, error)
}

// Source est une source de commits hybride (flux + polling).
type Source struct {
	cl        *kgo.Client
	poll      Poller
	reconcile time.Duration
	logger    *log.Logger

	mu            sync.Mutex
	commits       kafkaclient.GroupCommits
	fallback      map[string]bool
	groups        []kafkaclient.GroupView // métadonnées de groupe lues depuis le dernier appel de Groups
	groupFallback map[string]bool
	ready         bool
	lastReconcile time.Time
	stats         stats
	done          chan struct{}
}

type stats struct {
	records            int64
	tombstones         int64
	malformed          int64
	unknownKeys        map[int16]int64
	unknownValues      map[int16]int64
	groupRecords       int64
	groupUnknownValues map[int16]int64
	mismatches         int64
	reconciliations    int64
	lastRecordAt       time.Time
	lagOffsets         int64
	lastError          string
	errors             int64
	errorsSeen         int64 // erreurs déjà prises en compte par Commits
	groupErrorsSeen    int64 // erreurs déjà prises en compte par Groups
}

// New ouvre un consommateur direct sur __consumer_offsets à partir de la fin
// et démarre la lecture. La première demande de commits prend un instantané
// par polling, que le flux met ensuite à jour.
func New(ctx context.Context, opts []kgo.Opt, poll Poller, reconcile time.Duration, logger *log.Logger) (*Source, error) {
	opts = append(opts,
		kgo.ConsumeTopics(Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("consumer %s: %w", Topic, err)
	}
	s := newSource(cl, poll, reconcile, logger)
	go s.run(ctx)
	return s, nil
}

func newSource(cl *kgo.Client, poll Poller, reconcile time.Duration, logger *log.Logger) *Source {
	return &Source{
		cl: cl, poll: poll, reconcile: reconcile, logger: logger,
		commits:       make(kafkaclient.GroupCommits),
		fallback:      make(map[string]bool),
		groupFallback: make(map[string]bool),
		stats: stats{unknownKeys: make(map[int16]int64), unknownValues: make(map[int16]int64),
			groupUnknownValues: make(map[int16]int64)},
		done: make(chan struct{}),
	}
}

func (s *Source) run(ctx context.Context) {
	defer close(s.done)
	for {
		fetches := s.cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			s.mu.Lock()
			s.stats.errors++
			s.stats.lastError = fmt.Sprintf("%s/%d: %v", t, p, err)
			s.mu.Unlock()
		})
		var lag int64
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if n := len(p.Records); n > 0 {
				lag += p.HighWatermark - (p.Records[n-1].Offset + 1)
			}
		})
		s.mu.Lock()
		fetches.EachRecord(s.apply)
		if lag > 0 || fetches.NumRecords() > 0 {
			s.stats.lagOffsets = lag
		}
		s.mu.Unlock()
	}
}

// apply intègre un enregistrement ; s.mu est tenu par l'appelant.
func (s *Source) apply(r *kgo.Record) {
	if len(r.Key) < 2 {
		s.stats.malformed++
		return
	}
	s.stats.records++
	s.stats.lastRecordAt = r.Timestamp
	kv := int16(binary.BigEndian.Uint16(r.Key))
	switch {
	case kv <= keyVersionCommitMax:
		var k kmsg.OffsetCommitKey
		if err := k.ReadFrom(r.Key); err != nil {
			s.stats.malformed++
			return
		}
		if r.Value == nil {
			s.stats.tombstones++
			if t := s.commits[k.Group]; t != nil {
				delete(t[k.Topic], k.Partition)
				if len(t[k.Topic]) == 0 {
					delete(t, k.Topic)
				}
				if len(t) == 0 {
					delete(s.commits, k.Group)
				}
			}
			return
		}
		if len(r.Value) < 2 {
			s.stats.malformed++
			return
		}
		vv := int16(binary.BigEndian.Uint16(r.Value))
		if vv > valueVersionCommitMax {
			s.stats.unknownValues[vv]++
			if !s.fallback[k.Group] {
				s.fallback[k.Group] = true
				s.logger.Printf("stream %s: commit value with unknown version %d for group %s: group switched to polling", Topic, vv, k.Group)
			}
			return
		}
		var v kmsg.OffsetCommitValue
		if err := v.ReadFrom(r.Value); err != nil {
			s.stats.malformed++
			return
		}
		at := r.Timestamp
		if v.CommitTimestamp > 0 {
			at = time.UnixMilli(v.CommitTimestamp)
		}
		s.commits.Set(k.Group, k.Topic, k.Partition, kafkaclient.Commit{Offset: v.Offset, At: at, Metadata: v.Metadata})
	case kv == keyVersionGroupMeta:
		s.applyGroup(r)
	default:
		s.stats.unknownKeys[kv]++
	}
}

// applyGroup intègre un enregistrement de métadonnées de groupe classique ;
// s.mu est tenu par l'appelant.
func (s *Source) applyGroup(r *kgo.Record) {
	var k kmsg.GroupMetadataKey
	if err := k.ReadFrom(r.Key); err != nil {
		s.stats.malformed++
		return
	}
	s.stats.groupRecords++
	if r.Value == nil {
		s.stats.tombstones++
		s.groups = append(s.groups, kafkaclient.GroupView{Group: k.Group, At: r.Timestamp, Deleted: true})
		return
	}
	if len(r.Value) < 2 {
		s.stats.malformed++
		return
	}
	if vv := int16(binary.BigEndian.Uint16(r.Value)); vv > valueVersionGroupMax {
		s.stats.groupUnknownValues[vv]++
		if !s.groupFallback[k.Group] {
			s.groupFallback[k.Group] = true
			s.logger.Printf("stream %s: metadata with unknown version %d for group %s: members read with DescribeGroups", Topic, vv, k.Group)
		}
		return
	}
	var v kmsg.GroupMetadataValue
	if err := v.ReadFrom(r.Value); err != nil {
		s.stats.malformed++
		return
	}
	view := kafkaclient.GroupView{Group: k.Group, Generation: v.Generation, ProtocolType: v.ProtocolType, At: r.Timestamp}
	if v.Protocol != nil {
		view.Protocol = *v.Protocol
	}
	for _, m := range v.Members {
		mm := kafkaclient.Member{ID: m.MemberID, ClientID: m.ClientID, ClientHost: strings.TrimPrefix(m.ClientHost, "/")}
		if m.InstanceID != nil {
			mm.InstanceID = *m.InstanceID
		}
		if v.ProtocolType == "consumer" {
			kafkaclient.DecodeConsumerMember(&mm, m.Subscription, m.Assignment)
		}
		view.Members = append(view.Members, mm)
	}
	s.groups = append(s.groups, view)
}

// Groups renvoie, dans l'ordre du flux, les métadonnées de groupe lues
// depuis l'appel précédent, les groupes dont les métadonnées sont en
// version inconnue, et healthy = aucune erreur de lecture du flux depuis
// l'appel précédent (sinon des générations ont pu être retardées).
func (s *Source) Groups() ([]kafkaclient.GroupView, map[string]bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	views := s.groups
	s.groups = nil
	healthy := s.stats.errors == s.stats.groupErrorsSeen
	s.stats.groupErrorsSeen = s.stats.errors
	fallback := make(map[string]bool, len(s.groupFallback))
	for g := range s.groupFallback {
		fallback[g] = true
	}
	return views, fallback, healthy
}

// Commits renvoie la vue courante des groupes demandés. Au premier appel et
// à chaque échéance de réconciliation, la vue est remplacée par un
// instantané de l'API publique (le flux conserve l'heure des commits quand
// l'offset n'a pas changé) ; entre deux, seuls les groupes en repli sont
// interrogés par polling. Un flux en erreur depuis l'appel précédent ne fait
// plus foi : l'instantané est repris à chaque appel jusqu'à ce qu'il se rétablisse.
func (s *Source) Commits(ctx context.Context, groups []string) (kafkaclient.GroupCommits, map[string]error, error) {
	s.mu.Lock()
	streamFailed := s.stats.errors > s.stats.errorsSeen
	s.stats.errorsSeen = s.stats.errors
	needFull := !s.ready || streamFailed || time.Since(s.lastReconcile) >= s.reconcile
	var toPoll []string
	if !needFull {
		for _, g := range groups {
			if s.fallback[g] {
				toPoll = append(toPoll, g)
			}
		}
	}
	s.mu.Unlock()

	var groupErrs map[string]error
	switch {
	case needFull:
		polled, errs, err := s.poll.Commits(ctx, groups)
		if err != nil {
			if !s.isReady() {
				return nil, errs, err
			}
			s.logger.Printf("reconciliation failed, keeping the stream view: %v", err)
		} else {
			s.merge(groups, polled, errs, true)
		}
		groupErrs = errs
	case len(toPoll) > 0:
		polled, errs, err := s.poll.Commits(ctx, toPoll)
		if err != nil {
			s.logger.Printf("fallback group polling failed: %v", err)
			groupErrs = make(map[string]error, len(toPoll))
			for _, g := range toPoll {
				groupErrs[g] = err
			}
		} else {
			s.merge(toPoll, polled, errs, false)
			groupErrs = errs
		}
	}
	return s.view(groups), groupErrs, nil
}

func (s *Source) isReady() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready
}

// merge remplace la vue des groupes interrogés par le résultat du polling,
// sauf pour ceux en erreur ; elle compte les écarts avec le flux.
func (s *Source) merge(groups []string, polled kafkaclient.GroupCommits, errs map[string]error, full bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range groups {
		if errs[g] != nil {
			continue
		}
		polledTopics := polled[g]
		old := s.commits[g]
		if polledTopics == nil {
			delete(s.commits, g)
			continue
		}
		fresh := make(map[string]map[int32]kafkaclient.Commit, len(polledTopics))
		for t, parts := range polledTopics {
			cp := make(map[int32]kafkaclient.Commit, len(parts))
			for p, c := range parts {
				if prev, ok := old[t][p]; ok {
					if prev.Offset == c.Offset {
						c.At = prev.At
					} else if s.ready && !s.fallback[g] {
						s.stats.mismatches++
					}
				}
				cp[p] = c
			}
			fresh[t] = cp
		}
		s.commits[g] = fresh
	}
	if full {
		s.ready = true
		s.lastReconcile = time.Now()
		s.stats.reconciliations++
	}
}

// view copie la vue des groupes demandés.
func (s *Source) view(groups []string) kafkaclient.GroupCommits {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(kafkaclient.GroupCommits, len(groups))
	for _, g := range groups {
		topics := s.commits[g]
		if topics == nil {
			continue
		}
		ct := make(map[string]map[int32]kafkaclient.Commit, len(topics))
		for t, parts := range topics {
			cp := make(map[int32]kafkaclient.Commit, len(parts))
			for p, c := range parts {
				cp[p] = c
			}
			ct[t] = cp
		}
		out[g] = ct
	}
	return out
}

// Mode identifie la source pour les documents de santé.
func (s *Source) Mode() string { return "hybrid" }

// Stats expose l'état du parseur et du flux.
func (s *Source) Stats() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	unknownKeys := make(map[string]int64, len(s.stats.unknownKeys))
	for v, n := range s.stats.unknownKeys {
		unknownKeys[fmt.Sprint(v)] = n
	}
	unknownValues := make(map[string]int64, len(s.stats.unknownValues))
	for v, n := range s.stats.unknownValues {
		unknownValues[fmt.Sprint(v)] = n
	}
	var lastRecordAge any
	if !s.stats.lastRecordAt.IsZero() {
		lastRecordAge = time.Since(s.stats.lastRecordAt).Milliseconds()
	}
	return map[string]any{
		"ready":                   s.ready,
		"records":                 s.stats.records,
		"tombstones":              s.stats.tombstones,
		"malformed":               s.stats.malformed,
		"unknown_keys":            sum(s.stats.unknownKeys),
		"unknown_key_versions":    unknownKeys,
		"unknown_values":          sum(s.stats.unknownValues),
		"unknown_value_versions":  unknownValues,
		"groups_fallback":         len(s.fallback),
		"group_records":           s.stats.groupRecords,
		"group_unknown_values":    sum(s.stats.groupUnknownValues),
		"groups_members_fallback": len(s.groupFallback),
		"mismatches":              s.stats.mismatches,
		"reconciliations":         s.stats.reconciliations,
		"lag_offsets":             s.stats.lagOffsets,
		"last_record_age_ms":      lastRecordAge,
		"errors":                  s.stats.errors,
		"last_error":              s.stats.lastError,
	}
}

func sum(m map[int16]int64) int64 {
	var n int64
	for _, v := range m {
		n += v
	}
	return n
}

// Close arrête le consommateur.
func (s *Source) Close() {
	if s.cl != nil {
		s.cl.Close()
		<-s.done
	}
}
