package offsetstream

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/gnocent/kafkabeat/internal/kafkaclient"
)

type fakePoller struct {
	commits kafkaclient.GroupCommits
	errs    map[string]error
	calls   [][]string
}

func (f *fakePoller) Commits(_ context.Context, groups []string) (kafkaclient.GroupCommits, map[string]error, error) {
	f.calls = append(f.calls, groups)
	out := make(kafkaclient.GroupCommits)
	for _, g := range groups {
		if t := f.commits[g]; t != nil {
			out[g] = t
		}
	}
	return out, f.errs, nil
}

func commitRecord(group, topic string, partition int32, offset int64, at time.Time, valueVersion int16) *kgo.Record {
	k := kmsg.OffsetCommitKey{Version: 1, Group: group, Topic: topic, Partition: partition}
	var value []byte
	if offset >= 0 {
		v := kmsg.OffsetCommitValue{Version: 3, Offset: offset, LeaderEpoch: -1, CommitTimestamp: at.UnixMilli()}
		value = v.AppendTo(nil)
		if valueVersion > 0 {
			value[0], value[1] = byte(valueVersion>>8), byte(valueVersion)
		}
	}
	return &kgo.Record{Key: k.AppendTo(nil), Value: value, Timestamp: at}
}

func newTest() (*Source, *fakePoller) {
	fp := &fakePoller{commits: make(kafkaclient.GroupCommits)}
	s := newSource(nil, fp, time.Hour, log.New(os.Stderr, "", 0))
	return s, fp
}

func TestApplyAndFallback(t *testing.T) {
	s, _ := newTest()
	at := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	s.mu.Lock()
	s.apply(commitRecord("g", "t", 0, 42, at, 0))
	s.apply(commitRecord("g", "t", 1, 7, at, 0))
	s.apply(commitRecord("g", "t", 1, -1, at, 0)) // tombstone
	s.apply(groupRecord("g", 3, at))
	s.apply(&kgo.Record{Key: []byte{0, 9, 0, 0}, Value: []byte{0, 0}}) // version de clé inconnue
	s.apply(commitRecord("h", "t", 0, 1, at, 9))                       // version de valeur inconnue
	s.apply(&kgo.Record{Key: []byte{1}})
	s.mu.Unlock()

	c := s.commits["g"]["t"][0]
	if c.Offset != 42 || !c.At.Equal(at) {
		t.Fatalf("commit attendu 42 à %s : %+v", at, c)
	}
	if _, ok := s.commits["g"]["t"][1]; ok {
		t.Fatal("la tombstone doit supprimer le commit")
	}
	if !s.fallback["h"] || s.fallback["g"] {
		t.Fatalf("seul h doit être en repli : %v", s.fallback)
	}
	st := s.Stats()
	if st["unknown_keys"].(int64) != 1 || st["unknown_values"].(int64) != 1 || st["tombstones"].(int64) != 1 || st["malformed"].(int64) != 1 || st["records"].(int64) != 6 {
		t.Fatalf("compteurs inattendus : %v", st)
	}
}

func TestCommitsSnapshotThenStreamThenFallback(t *testing.T) {
	s, fp := newTest()
	at := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	fp.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 10})
	fp.commits.Set("h", "t", 0, kafkaclient.Commit{Offset: 5})

	// 1. premier appel : instantané par polling
	got, errs, err := s.Commits(context.Background(), []string{"g", "h"})
	if err != nil || len(errs) != 0 || got["g"]["t"][0].Offset != 10 || got["h"]["t"][0].Offset != 5 {
		t.Fatalf("instantané attendu : %v %v %v", got, errs, err)
	}
	if len(fp.calls) != 1 {
		t.Fatalf("un seul polling attendu : %v", fp.calls)
	}

	// 2. le flux avance g ; h passe en repli sur une version inconnue
	s.mu.Lock()
	s.apply(commitRecord("g", "t", 0, 11, at, 0))
	s.apply(commitRecord("h", "t", 0, 6, at, 9))
	s.mu.Unlock()
	fp.commits.Set("h", "t", 0, kafkaclient.Commit{Offset: 6})
	got, _, _ = s.Commits(context.Background(), []string{"g", "h"})
	if got["g"]["t"][0].Offset != 11 || !got["g"]["t"][0].At.Equal(at) {
		t.Fatalf("g doit venir du flux avec l'heure du commit : %+v", got["g"])
	}
	if got["h"]["t"][0].Offset != 6 || len(fp.calls) != 2 || len(fp.calls[1]) != 1 || fp.calls[1][0] != "h" {
		t.Fatalf("h doit venir du polling de repli : %+v, appels %v", got["h"], fp.calls)
	}
	// la copie renvoyée est indépendante de la vue interne
	got["g"]["t"][0] = kafkaclient.Commit{Offset: 0}
	if s.commits["g"]["t"][0].Offset != 11 {
		t.Fatal("la vue interne ne doit pas être modifiable de l'extérieur")
	}

	// 3. réconciliation : le polling gagne, l'heure du flux est gardée si l'offset est égal
	s.reconcile = 0
	fp.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 11})
	fp.commits.Set("g", "t", 1, kafkaclient.Commit{Offset: 3})
	got, _, _ = s.Commits(context.Background(), []string{"g", "h"})
	if got["g"]["t"][1].Offset != 3 || !got["g"]["t"][0].At.Equal(at) {
		t.Fatalf("réconciliation attendue : %+v", got["g"])
	}
	fp.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 20})
	got, _, _ = s.Commits(context.Background(), []string{"g"})
	if got["g"]["t"][0].Offset != 20 || s.Stats()["mismatches"].(int64) != 1 {
		t.Fatalf("écart flux/polling attendu : %+v, %v", got["g"], s.Stats())
	}

	// 4. un groupe en erreur côté polling garde la vue du flux
	fp.errs = map[string]error{"g": context.DeadlineExceeded}
	got, errs, _ = s.Commits(context.Background(), []string{"g"})
	if errs["g"] == nil || got["g"]["t"][0].Offset != 20 {
		t.Fatalf("groupe en erreur : vue conservée attendue : %v %v", got, errs)
	}
}

func TestCommitsPollAllWhileStreamFails(t *testing.T) {
	s, fp := newTest()
	fp.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 10})
	s.Commits(context.Background(), []string{"g"})
	s.Commits(context.Background(), []string{"g"})
	if len(fp.calls) != 1 {
		t.Fatalf("flux sain : un seul instantané attendu, appels %v", fp.calls)
	}
	// le flux échoue (ACL, broker) : chaque appel reprend l'instantané complet
	s.mu.Lock()
	s.stats.errors++
	s.mu.Unlock()
	fp.commits.Set("g", "t", 0, kafkaclient.Commit{Offset: 12})
	got, _, _ := s.Commits(context.Background(), []string{"g"})
	if len(fp.calls) != 2 || got["g"]["t"][0].Offset != 12 {
		t.Fatalf("flux en erreur : polling complet attendu, appels %v, vue %+v", fp.calls, got["g"])
	}
	// rétabli : plus de polling hors réconciliation
	s.Commits(context.Background(), []string{"g"})
	if len(fp.calls) != 2 {
		t.Fatalf("flux rétabli : pas de polling attendu, appels %v", fp.calls)
	}
}

// groupRecord construit l'enregistrement GroupMetadata (valeur v3) d'un
// groupe consumer à deux membres ; generation < 0 donne une tombstone.
func groupRecord(group string, generation int32, at time.Time) *kgo.Record {
	k := kmsg.GroupMetadataKey{Version: 2, Group: group}
	if generation < 0 {
		return &kgo.Record{Key: k.AppendTo(nil), Timestamp: at}
	}
	v := kmsg.NewGroupMetadataValue()
	v.Version = 3
	v.ProtocolType = "consumer"
	v.Generation = generation
	protocol, leader := "range", "m1"
	v.Protocol, v.Leader = &protocol, &leader
	v.CurrentStateTimestamp = at.UnixMilli()
	for i, parts := range [][]int32{{0, 1}, {2}} {
		sub := kmsg.ConsumerMemberMetadata{Version: 1, Topics: []string{"t"}}
		asg := kmsg.ConsumerMemberAssignment{Version: 1, Topics: []kmsg.ConsumerMemberAssignmentTopic{{Topic: "t", Partitions: parts}}}
		m := kmsg.NewGroupMetadataValueMember()
		m.MemberID = fmt.Sprintf("m%d", i+1)
		m.ClientID = fmt.Sprintf("client-%d", i+1)
		m.ClientHost = "/10.0.0.1"
		m.SessionTimeoutMillis = 45000
		m.Subscription = sub.AppendTo(nil)
		m.Assignment = asg.AppendTo(nil)
		v.Members = append(v.Members, m)
	}
	return &kgo.Record{Key: k.AppendTo(nil), Value: v.AppendTo(nil), Timestamp: at}
}

func TestGroupMetadata(t *testing.T) {
	s, _ := newTest()
	at := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	s.mu.Lock()
	s.apply(groupRecord("g", 4, at))
	unknown := groupRecord("h", 1, at)
	unknown.Value[0], unknown.Value[1] = 0, 9 // version de valeur inconnue
	s.apply(unknown)
	s.apply(groupRecord("g", -1, at.Add(time.Second)))
	s.mu.Unlock()

	views, fallback, healthy := s.Groups()
	if !healthy || len(views) != 2 || !fallback["h"] || fallback["g"] {
		t.Fatalf("2 vues, h en repli, flux sain attendus : %+v %v %v", views, fallback, healthy)
	}
	v := views[0]
	if v.Group != "g" || v.Generation != 4 || v.Protocol != "range" || v.ProtocolType != "consumer" || !v.At.Equal(at) || len(v.Members) != 2 {
		t.Fatalf("vue inattendue : %+v", v)
	}
	m := v.Members[1]
	if m.ID != "m2" || m.ClientID != "client-2" || m.ClientHost != "10.0.0.1" || len(m.Topics) != 1 || m.Topics[0] != "t" ||
		len(m.Assigned["t"]) != 1 || m.Assigned["t"][0] != 2 {
		t.Fatalf("membre inattendu : %+v", m)
	}
	if !views[1].Deleted || views[1].Group != "g" {
		t.Fatalf("tombstone attendue : %+v", views[1])
	}
	st := s.Stats()
	if st["group_records"].(int64) != 3 || st["group_unknown_values"].(int64) != 1 || st["groups_members_fallback"].(int) != 1 {
		t.Fatalf("compteurs inattendus : %v", st)
	}

	// vidé par l'appel précédent ; une erreur de lecture rend le flux douteux une fois
	s.mu.Lock()
	s.stats.errors++
	s.mu.Unlock()
	if views, _, healthy := s.Groups(); len(views) != 0 || healthy {
		t.Fatalf("aucune vue et flux en erreur attendus : %v %v", views, healthy)
	}
	if _, _, healthy := s.Groups(); !healthy {
		t.Fatal("sans nouvelle erreur, le flux redevient sain")
	}
}
