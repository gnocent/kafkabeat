// Package kafkaclient enveloppe franz-go : métadonnées, offsets de fin et de
// début, groupes et offsets commités, en requêtes groupées par broker ou par
// coordinateur.
package kafkaclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/gnocent/kafkabeat/internal/config"
)

// Client est une connexion au cluster supervisé.
type Client struct {
	cl  *kgo.Client
	adm *kadm.Client
}

// Options construit les options de connexion (brokers, TLS, SASL) communes
// au client d'administration et au consommateur du flux.
func Options(cfg config.Kafka) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequestTimeoutOverhead(10 * time.Second),
		// un cluster injoignable doit faire échouer le cycle vite : le cycle
		// suivant réessaie et le document de santé porte l'erreur
		kgo.RetryTimeout(10 * time.Second),
	}
	if cfg.TLS.Enabled {
		tc, err := TLSConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	}
	switch cfg.SASL.Mechanism {
	case "":
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: cfg.SASL.Username, Pass: cfg.SASL.Password}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: cfg.SASL.Username, Pass: cfg.SASL.Password}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: cfg.SASL.Username, Pass: cfg.SASL.Password}.AsSha512Mechanism()))
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %q", cfg.SASL.Mechanism)
	}
	return opts, nil
}

// New ouvre la connexion (TLS et SASL selon la configuration).
func New(cfg config.Kafka) (*Client, error) {
	opts, err := Options(cfg)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Client{cl: cl, adm: kadm.NewClient(cl)}, nil
}

// TLSConfig construit une configuration TLS à partir des fichiers indiqués.
func TLSConfig(t config.TLS) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s: no readable CA certificate", t.CAFile)
		}
		tc.RootCAs = pool
	}
	if t.CertFile != "" || t.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, err
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

// Ping vérifie qu'un broker répond.
func (c *Client) Ping(ctx context.Context) error { return c.cl.Ping(ctx) }

// Close ferme la connexion.
func (c *Client) Close() { c.cl.Close() }

// PartitionMeta décrit une partition.
type PartitionMeta struct {
	ID       int32
	Leader   int32
	Replicas []int32
	ISR      []int32
}

// TopicMeta décrit un topic ; ID est l'identifiant Kafka en base64 (comme kafka-topics --describe).
type TopicMeta struct {
	Name       string
	ID         string
	Internal   bool
	Partitions []PartitionMeta
	Err        error
}

// BrokerMeta décrit un broker.
type BrokerMeta struct {
	ID   int32
	Host string
	Port int32
}

// Metadata est l'inventaire du cluster.
type Metadata struct {
	ClusterID string
	Brokers   []BrokerMeta
	Topics    map[string]TopicMeta
}

// Metadata interroge l'inventaire complet du cluster.
func (c *Client) Metadata(ctx context.Context) (Metadata, error) {
	md, err := c.adm.Metadata(ctx)
	if err != nil {
		return Metadata{}, err
	}
	out := Metadata{ClusterID: md.Cluster, Topics: make(map[string]TopicMeta, len(md.Topics))}
	for _, b := range md.Brokers {
		out.Brokers = append(out.Brokers, BrokerMeta{ID: b.NodeID, Host: b.Host, Port: b.Port})
	}
	for name, t := range md.Topics {
		tm := TopicMeta{Name: name, ID: TopicIDString(t.ID), Internal: t.IsInternal, Err: t.Err}
		for _, p := range t.Partitions {
			tm.Partitions = append(tm.Partitions, PartitionMeta{ID: p.Partition, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR})
		}
		sort.Slice(tm.Partitions, func(i, j int) bool { return tm.Partitions[i].ID < tm.Partitions[j].ID })
		out.Topics[name] = tm
	}
	return out, nil
}

// Brokers renvoie l'identifiant du cluster et ses brokers (métadonnées sans topic).
func (c *Client) Brokers(ctx context.Context) (string, []BrokerMeta, error) {
	md, err := c.adm.BrokerMetadata(ctx)
	if err != nil {
		return "", nil, err
	}
	out := make([]BrokerMeta, 0, len(md.Brokers))
	for _, b := range md.Brokers {
		out = append(out, BrokerMeta{ID: b.NodeID, Host: b.Host, Port: b.Port})
	}
	return md.Cluster, out, nil
}

// TopicIDString encode un identifiant de topic comme Kafka l'affiche.
func TopicIDString(id [16]byte) string {
	if id == [16]byte{} {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(id[:])
}

// Offsets : topic -> partition -> offset.
type Offsets map[string]map[int32]int64

func (o Offsets) set(topic string, partition int32, offset int64) {
	m := o[topic]
	if m == nil {
		m = make(map[int32]int64)
		o[topic] = m
	}
	m[partition] = offset
}

// Get renvoie l'offset d'une partition.
func (o Offsets) Get(topic string, partition int32) (int64, bool) {
	v, ok := o[topic][partition]
	return v, ok
}

// EndOffsets renvoie les offsets de fin (LEO) des topics demandés, en une
// requête par broker. Les erreurs partielles (partition ou broker) sont
// renvoyées à part ; err n'est non nul que si rien n'a pu être lu.
func (c *Client) EndOffsets(ctx context.Context, topics []string) (Offsets, []error, error) {
	lo, err := c.adm.ListEndOffsets(ctx, topics...)
	return listed(lo, err)
}

// StartOffsets renvoie les offsets de début (plus ancien message).
func (c *Client) StartOffsets(ctx context.Context, topics []string) (Offsets, []error, error) {
	lo, err := c.adm.ListStartOffsets(ctx, topics...)
	return listed(lo, err)
}

func listed(lo kadm.ListedOffsets, err error) (Offsets, []error, error) {
	var partial []error
	if err != nil {
		var se *kadm.ShardErrors
		if !errors.As(err, &se) || se.AllFailed {
			return nil, nil, err
		}
		for _, e := range se.Errs {
			partial = append(partial, fmt.Errorf("broker %d: %w", e.Broker.NodeID, e.Err))
		}
	}
	out := make(Offsets, len(lo))
	lo.Each(func(l kadm.ListedOffset) {
		if l.Err != nil {
			if l.Partition >= 0 {
				partial = append(partial, fmt.Errorf("%s/%d: %w", l.Topic, l.Partition, l.Err))
			}
			return
		}
		out.set(l.Topic, l.Partition, l.Offset)
	})
	return out, partial, nil
}

// Group résume un groupe listé. Type vient de ListGroups v5 (Kafka 3.8+) :
// classic, consumer (protocole KIP-848), share ou streams ; vide avant.
type Group struct {
	State       string
	Coordinator int32
	Type        string
}

// ListGroups liste tous les groupes avec leur état (Empty, Stable, Dead...)
// et leur type, que kadm ne rend pas : requête brute, une par broker. Les
// brokers en échec sont renvoyés à part (leurs groupes manquent à la liste) ;
// err n'est non nul que si tous ont échoué.
func (c *Client) ListGroups(ctx context.Context) (map[string]Group, []error, error) {
	shards := c.cl.RequestSharded(ctx, kmsg.NewPtrListGroupsRequest())
	if len(shards) == 0 {
		return nil, nil, errors.New("no reachable broker")
	}
	out := make(map[string]Group)
	var errs []error
	for _, sh := range shards {
		if sh.Err != nil {
			errs = append(errs, fmt.Errorf("broker %d: %w", sh.Meta.NodeID, sh.Err))
			continue
		}
		resp, ok := sh.Resp.(*kmsg.ListGroupsResponse)
		if !ok {
			continue
		}
		if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
			errs = append(errs, fmt.Errorf("broker %d: %w", sh.Meta.NodeID, err))
			continue
		}
		for _, g := range resp.Groups {
			out[g.Group] = Group{State: g.GroupState, Coordinator: sh.Meta.NodeID, Type: g.GroupType}
		}
	}
	if len(errs) > 0 && len(errs) == len(shards) {
		return nil, nil, errors.Join(errs...)
	}
	return out, errs, nil
}

// GroupsUnavailable départage, parmi des groupes connus mais absents de
// ListGroups, ceux dont le coordinateur ne peut pas répondre : pendant le
// chargement qui suit la reprise d'un broker, ListGroups les omet sans
// erreur, et seul DescribeGroups le révèle (coordinateur en chargement,
// indisponible, pas coordinateur, ou broker injoignable). Renvoie l'erreur par
// groupe concerné ; un groupe absent du résultat a bien disparu.
func (c *Client) GroupsUnavailable(ctx context.Context, groups []string) map[string]error {
	out := make(map[string]error)
	if len(groups) == 0 {
		return out
	}
	dg, err := c.adm.DescribeGroups(ctx, groups...)
	if err != nil {
		var se *kadm.ShardErrors
		if !errors.As(err, &se) {
			for _, g := range groups {
				out[g] = err
			}
			return out
		}
		for _, e := range se.Errs {
			if req, ok := e.Req.(*kmsg.DescribeGroupsRequest); ok {
				for _, g := range req.Groups {
					out[g] = fmt.Errorf("broker %d: %w", e.Broker.NodeID, e.Err)
				}
			}
		}
		if se.AllFailed && len(out) == 0 {
			for _, g := range groups {
				out[g] = err
			}
			return out
		}
	}
	for name, g := range dg {
		if g.Err != nil && (errors.Is(g.Err, kerr.CoordinatorLoadInProgress) || errors.Is(g.Err, kerr.CoordinatorNotAvailable) || errors.Is(g.Err, kerr.NotCoordinator)) {
			out[name] = g.Err
		}
	}
	return out
}

// Member est un membre d'un groupe classique. Topics (abonnements) et
// Assigned (topic -> partitions) ne sont renseignés que pour le protocole
// consumer.
type Member struct {
	ID         string
	InstanceID string
	ClientID   string
	ClientHost string
	Topics     []string
	Assigned   map[string][]int32
}

// GroupView décrit les membres d'un groupe classique à l'instant At : lus
// dans un enregistrement GroupMetadata du flux (Generation connue), ou par
// DescribeGroups (Generation -1, State renseigné ; abonnements et
// assignations ne sont fiables qu'en état Stable). Deleted : le groupe a
// été supprimé (tombstone du flux).
type GroupView struct {
	Group        string
	Generation   int32
	ProtocolType string
	Protocol     string
	State        string
	At           time.Time
	Members      []Member
	Deleted      bool
}

// Members décrit les groupes demandés (DescribeGroups, une requête par
// coordinateur). Les groupes en erreur sont omis ; err peut accompagner un
// résultat partiel (coordinateurs en échec).
func (c *Client) Members(ctx context.Context, groups []string) (map[string]GroupView, error) {
	out := make(map[string]GroupView)
	if len(groups) == 0 {
		return out, nil
	}
	at := time.Now()
	dg, err := c.adm.DescribeGroups(ctx, groups...)
	if err != nil {
		var se *kadm.ShardErrors
		if !errors.As(err, &se) || se.AllFailed {
			return nil, err
		}
	}
	for name, g := range dg {
		if g.Err != nil {
			continue
		}
		v := GroupView{Group: name, Generation: -1, ProtocolType: g.ProtocolType, Protocol: g.Protocol, State: g.State, At: at}
		for _, m := range g.Members {
			// Kafka renvoie « /adresse » (InetAddress.toString) ; Metricbeat publie l'adresse seule
			mm := Member{ID: m.MemberID, ClientID: m.ClientID, ClientHost: strings.TrimPrefix(m.ClientHost, "/")}
			if m.InstanceID != nil {
				mm.InstanceID = *m.InstanceID
			}
			if j, ok := m.Join.AsConsumer(); ok {
				mm.Topics = j.Topics
			}
			if a, ok := m.Assigned.AsConsumer(); ok {
				mm.Assigned = assignedMap(a)
			}
			v.Members = append(v.Members, mm)
		}
		out[name] = v
	}
	return out, err
}

func assignedMap(a *kmsg.ConsumerMemberAssignment) map[string][]int32 {
	out := make(map[string][]int32, len(a.Topics))
	for _, t := range a.Topics {
		out[t.Topic] = append(out[t.Topic], t.Partitions...)
	}
	return out
}

// DecodeConsumerMember complète un membre avec l'abonnement et l'assignation
// sérialisés du protocole consumer (enregistrement GroupMetadata) ; un
// contenu illisible est ignoré.
func DecodeConsumerMember(m *Member, subscription, assignment []byte) {
	var s kmsg.ConsumerMemberMetadata
	if len(subscription) > 0 && s.ReadFrom(subscription) == nil {
		m.Topics = s.Topics
	}
	var a kmsg.ConsumerMemberAssignment
	if len(assignment) > 0 && a.ReadFrom(assignment) == nil {
		m.Assigned = assignedMap(&a)
	}
}

// Commit est un offset commité ; At est l'heure du commit quand la source la
// connaît (flux), sinon zéro ; Metadata est la chaîne libre jointe au commit.
type Commit struct {
	Offset   int64
	At       time.Time
	Metadata string
}

// GroupCommits : groupe -> topic -> partition -> commit.
type GroupCommits map[string]map[string]map[int32]Commit

// Set enregistre un commit.
func (g GroupCommits) Set(group, topic string, partition int32, c Commit) {
	t := g[group]
	if t == nil {
		t = make(map[string]map[int32]Commit)
		g[group] = t
	}
	p := t[topic]
	if p == nil {
		p = make(map[int32]Commit)
		t[topic] = p
	}
	p[partition] = c
}

// Commits interroge les offsets commités de tous les topics des groupes
// demandés (OffsetFetch v8+, plusieurs groupes par requête, une requête par
// coordinateur). Les erreurs par groupe sont renvoyées à part.
func (c *Client) Commits(ctx context.Context, groups []string) (GroupCommits, map[string]error, error) {
	out := make(GroupCommits, len(groups))
	groupErrs := make(map[string]error)
	if len(groups) == 0 {
		return out, groupErrs, nil
	}
	req := kmsg.NewPtrOffsetFetchRequest()
	for _, g := range groups {
		rg := kmsg.NewOffsetFetchRequestGroup()
		rg.Group = g
		req.Groups = append(req.Groups, rg)
	}
	shards := c.cl.RequestSharded(ctx, req)
	var failed int
	for _, sh := range shards {
		sent, _ := sh.Req.(*kmsg.OffsetFetchRequest)
		if sh.Err != nil {
			failed++
			if sent != nil {
				for _, g := range sent.Groups {
					groupErrs[g.Group] = sh.Err
				}
				if sent.Group != "" {
					groupErrs[sent.Group] = sh.Err
				}
			}
			continue
		}
		resp, ok := sh.Resp.(*kmsg.OffsetFetchResponse)
		if !ok {
			continue
		}
		if len(resp.Groups) > 0 {
			for _, g := range resp.Groups {
				if err := kerr.ErrorForCode(g.ErrorCode); err != nil {
					groupErrs[g.Group] = err
					continue
				}
				for _, t := range g.Topics {
					for _, p := range t.Partitions {
						if err := kerr.ErrorForCode(p.ErrorCode); err != nil || p.Offset < 0 {
							continue
						}
						out.Set(g.Group, t.Topic, p.Partition, Commit{Offset: p.Offset, Metadata: deref(p.Metadata)})
					}
				}
			}
			continue
		}
		// Réponse v0-7 : un seul groupe par requête.
		group := ""
		if sent != nil {
			group = sent.Group
		}
		if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
			groupErrs[group] = err
			continue
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil || p.Offset < 0 {
					continue
				}
				out.Set(group, t.Topic, p.Partition, Commit{Offset: p.Offset, Metadata: deref(p.Metadata)})
			}
		}
	}
	if len(shards) > 0 && failed == len(shards) {
		return nil, groupErrs, fmt.Errorf("OffsetFetch: all requests failed (%d)", failed)
	}
	return out, groupErrs, nil
}

// Mode identifie la source pour les documents de santé.
func (c *Client) Mode() string { return "polling" }

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
