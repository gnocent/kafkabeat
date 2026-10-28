package kafkaclient

import (
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestListedOffsetsPartialErrors(t *testing.T) {
	lo := kadm.ListedOffsets{
		"t": {
			0: {Topic: "t", Partition: 0, Offset: 10},
			1: {Topic: "t", Partition: 1, Offset: -1, Err: errors.New("leader indisponible")},
		},
		"u": {0: {Topic: "u", Partition: 0, Offset: 7}},
	}
	// un broker en échec sur deux : résultat partiel, erreurs à part
	shardErr := &kadm.ShardErrors{Name: "ListOffsets", Errs: []kadm.ShardError{{Broker: kadm.BrokerDetail{NodeID: 2}, Err: errors.New("injoignable")}}}
	out, partial, err := listed(lo, shardErr)
	if err != nil {
		t.Fatalf("un échec partiel ne doit pas être une erreur : %v", err)
	}
	if off, ok := out.Get("t", 0); !ok || off != 10 {
		t.Fatalf("t/0 attendu à 10 : %v %v", off, ok)
	}
	if _, ok := out.Get("t", 1); ok {
		t.Fatal("une partition en erreur ne doit pas avoir d'offset")
	}
	if len(partial) != 2 || !strings.Contains(partial[0].Error(), "broker 2") || !strings.Contains(partial[1].Error(), "t/1") {
		t.Fatalf("erreurs partielles attendues (broker 2, t/1) : %v", partial)
	}
	// tous les brokers en échec : erreur
	if _, _, err := listed(nil, &kadm.ShardErrors{AllFailed: true, Errs: shardErr.Errs}); err == nil {
		t.Fatal("l'échec de tous les brokers doit être une erreur")
	}
	// erreur qui n'est pas un découpage par broker : erreur telle quelle
	if _, _, err := listed(nil, errors.New("délai dépassé")); err == nil {
		t.Fatal("une erreur globale doit être renvoyée")
	}
	// erreur de chargement sans partition (-1) : ignorée
	out, partial, err = listed(kadm.ListedOffsets{"t": {-1: {Topic: "t", Partition: -1, Err: errors.New("topic inconnu")}}}, nil)
	if err != nil || len(partial) != 0 || len(out) != 0 {
		t.Fatalf("une erreur de topic sans partition doit être ignorée : %v %v %v", out, partial, err)
	}
}

func TestTopicIDString(t *testing.T) {
	if TopicIDString([16]byte{}) != "" {
		t.Fatal("identifiant nul : chaîne vide attendue")
	}
	id := [16]byte{0xe4, 0xbe, 0x8d, 0xe0, 0x72, 0x59, 0x41, 0xc9, 0x9e, 0x30, 0x6b, 0xdf, 0xfe, 0x3f, 0x9e, 0xa8}
	// même encodage que kafka-topics --describe (base64 URL sans remplissage, 22 caractères)
	if s := TopicIDString(id); len(s) != 22 || strings.ContainsAny(s, "=+/") {
		t.Fatalf("encodage inattendu : %q", s)
	}
}

func TestDecodeConsumerMember(t *testing.T) {
	sub := kmsg.ConsumerMemberMetadata{Version: 1, Topics: []string{"t", "u"}}
	asg := kmsg.ConsumerMemberAssignment{Version: 1, Topics: []kmsg.ConsumerMemberAssignmentTopic{{Topic: "t", Partitions: []int32{0, 2}}}}
	var m Member
	DecodeConsumerMember(&m, sub.AppendTo(nil), asg.AppendTo(nil))
	if len(m.Topics) != 2 || m.Topics[0] != "t" || len(m.Assigned["t"]) != 2 || m.Assigned["t"][1] != 2 {
		t.Fatalf("abonnements et assignation attendus : %+v", m)
	}
	// contenu illisible ou vide : membre inchangé
	var empty Member
	DecodeConsumerMember(&empty, []byte{0xff}, nil)
	if empty.Topics != nil || empty.Assigned != nil {
		t.Fatalf("un contenu illisible doit être ignoré : %+v", empty)
	}
}

func TestOffsetsAndCommits(t *testing.T) {
	o := make(Offsets)
	o.set("t", 3, 42)
	if v, ok := o.Get("t", 3); !ok || v != 42 {
		t.Fatalf("offset attendu 42 : %v %v", v, ok)
	}
	if _, ok := o.Get("absent", 0); ok {
		t.Fatal("topic absent : aucun offset")
	}
	g := make(GroupCommits)
	g.Set("g", "t", 1, Commit{Offset: 5, Metadata: "m"})
	g.Set("g", "t", 2, Commit{Offset: 6})
	if len(g["g"]["t"]) != 2 || g["g"]["t"][1].Metadata != "m" {
		t.Fatalf("commits attendus : %+v", g)
	}
}
