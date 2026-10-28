package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// balancerFor traduit --balancer : vide = défaut de franz-go
// (cooperative-sticky) ; range, roundrobin, sticky et cooperative-sticky ;
// omit=<topic>/<partition> = tourniquet qui n'assigne jamais cette
// partition (partition sans membre hors rebalance). Tous les membres d'un
// groupe doivent annoncer le même balancer.
func balancerFor(spec string) (kgo.GroupBalancer, error) {
	switch {
	case spec == "":
		return nil, nil
	case spec == "range":
		return kgo.RangeBalancer(), nil
	case spec == "roundrobin":
		return kgo.RoundRobinBalancer(), nil
	case spec == "sticky":
		return kgo.StickyBalancer(), nil
	case spec == "cooperative-sticky":
		return kgo.CooperativeStickyBalancer(), nil
	case strings.HasPrefix(spec, "omit="):
		topic, p, ok := strings.Cut(strings.TrimPrefix(spec, "omit="), "/")
		n, err := strconv.ParseInt(p, 10, 32)
		if !ok || topic == "" || err != nil {
			return nil, fmt.Errorf("--balancer: expected omit=<topic>/<partition>, got %q", spec)
		}
		return omitBalancer{topic: topic, partition: int32(n)}, nil
	}
	return nil, fmt.Errorf("unknown balancer %q", spec)
}

// omitBalancer répartit en tourniquet toutes les partitions des topics
// abonnés, sauf une (protocole eager).
type omitBalancer struct {
	topic     string
	partition int32
}

func (omitBalancer) ProtocolName() string { return "kafkabeat-omit" }
func (omitBalancer) IsCooperative() bool  { return false }

func (omitBalancer) JoinGroupMetadata(topics []string, _ map[string][]int32, generation int32) []byte {
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 3
	meta.Topics = topics
	meta.Generation = generation
	return meta.AppendTo(nil)
}

func (omitBalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
	return kgo.ParseConsumerSyncAssignment(assignment)
}

func (o omitBalancer) MemberBalancer(members []kmsg.JoinGroupResponseMember) (kgo.GroupMemberBalancer, map[string]struct{}, error) {
	b, err := kgo.NewConsumerBalancer(o, members)
	if err != nil {
		return nil, nil, err
	}
	return b, b.MemberTopics(), nil
}

func (o omitBalancer) Balance(b *kgo.ConsumerBalancer, topics map[string]int32) kgo.IntoSyncAssignment {
	var parts []tp
	for topic := range b.MemberTopics() {
		for p := int32(0); p < topics[topic]; p++ {
			if topic != o.topic || p != o.partition {
				parts = append(parts, tp{topic, p})
			}
		}
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].topic < parts[j].topic || parts[i].topic == parts[j].topic && parts[i].partition < parts[j].partition
	})
	plan := b.NewPlan()
	for i, p := range parts {
		member, _ := b.MemberAt(i % len(b.Members()))
		plan.AddPartition(member, p.topic, p.partition)
	}
	return plan
}
