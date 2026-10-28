package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// États exposés par GET /status.
const (
	stateStarting  = "starting"
	stateRunning   = "running"
	statePaused    = "paused"
	stateResetting = "resetting"
	stateStopped   = "stopped"
)

type tp struct {
	topic     string
	partition int32
}

type consumedOffset struct {
	offset int64 // dernier offset consommé
	epoch  int32 // leader epoch du record, transmis au commit
}

type resetRequest struct {
	spec  string // earliest | latest | offset=<n>
	reply chan error
}

// consumer est un group consumer pilotable : une session franz-go à la fois,
// remplacée à chaque reset (quitter le groupe, réécrire les offsets, rejoindre).
type consumer struct {
	bootstrap      string
	group          string
	topics         []string
	commitMode     string
	commitInterval time.Duration
	maxRate        float64
	balancer       kgo.GroupBalancer // nil : défaut de franz-go
	sessionTimeout time.Duration     // 0 : défaut de franz-go
	revokeDelay    time.Duration     // retient chaque rebalance (révocation retardée)
	assign         bool              // sans adhésion au groupe : toutes les partitions, commits pour --group
	serverSide     bool              // protocole consumer (KIP-848) : assignation faite par le broker

	resetCh chan resetRequest
	wake    chan struct{} // réveille la boucle quand pause/resume change

	mu            sync.Mutex
	cl            *kgo.Client // session en cours, nil pendant un reset
	state         string
	paused        bool
	session       int
	consumed      map[tp]consumedOffset
	committed     map[tp]int64 // offset commité (prochain à consommer)
	total         int64
	lastCommitAt  time.Time
	lastCommitErr string
	assigned      map[tp]bool // partitions assignées par le groupe
	assignedAt    time.Time   // dernière assignation reçue
}

// runConsume traite `harness consume`.
func runConsume(ctx context.Context, args []string) error {
	fs, bootstrap := newFlagSet("consume")
	group := fs.String("group", "", "consumer group")
	topics := fs.String("topics", "", "topics to consume, comma-separated")
	commitMode := fs.String("commit", "auto", "commit mode: auto (periodic) or manual (POST /commit only)")
	commitInterval := fs.Duration("commit-interval", 5*time.Second, "commit period in auto mode")
	maxRate := fs.Float64("max-rate", 0, "maximum consume rate in msgs/s (0 = unlimited)")
	control := fs.String("control", "127.0.0.1:8181", "listen address of the HTTP control API")
	balancer := fs.String("balancer", "", "range | roundrobin | sticky | cooperative-sticky | omit=<topic>/<partition> (franz-go default)")
	sessionTimeout := fs.Duration("session-timeout", 0, "member session timeout (0 = default, 45s)")
	revokeDelay := fs.Duration("revoke-delay", 0, "delays the revocation of each rebalance (group held in PreparingRebalance)")
	assign := fs.Bool("assign", false, "consumes all partitions without joining the group and commits for --group (assign())")
	serverSide := fs.Bool("server-side", false, "joins the group with the consumer protocol (KIP-848, Kafka 4.0+, range balancer by default; classic fallback before)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	gb, err := balancerFor(*balancer)
	if err != nil {
		return err
	}
	if *serverSide {
		// franz-go n'utilise le protocole consumer qu'avec range ou sticky
		switch {
		case *assign:
			return errors.New("--server-side and --assign are mutually exclusive")
		case *balancer == "":
			gb = kgo.RangeBalancer()
		case *balancer != "range" && *balancer != "sticky":
			return errors.New("--server-side requires --balancer range or sticky")
		}
	}
	if *group == "" || *topics == "" {
		return errors.New("--group and --topics are required")
	}
	if *commitMode != "auto" && *commitMode != "manual" {
		return errors.New("--commit must be auto or manual")
	}
	if *commitInterval <= 0 {
		return errors.New("--commit-interval must be > 0")
	}
	if *maxRate < 0 {
		return errors.New("--max-rate must be >= 0")
	}

	c := &consumer{
		bootstrap:      *bootstrap,
		group:          *group,
		topics:         splitList(*topics),
		commitMode:     *commitMode,
		commitInterval: *commitInterval,
		maxRate:        *maxRate,
		balancer:       gb,
		sessionTimeout: *sessionTimeout,
		revokeDelay:    *revokeDelay,
		assign:         *assign,
		serverSide:     *serverSide,
		resetCh:        make(chan resetRequest),
		wake:           make(chan struct{}, 1),
		state:          stateStarting,
		consumed:       make(map[tp]consumedOffset),
		committed:      make(map[tp]int64),
		assigned:       make(map[tp]bool),
	}

	ln, err := net.Listen("tcp", *control)
	if err != nil {
		return fmt.Errorf("control API: %w", err)
	}
	srv := &http.Server{Handler: c.handler(ctx)}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("control API: %v", err)
		}
	}()
	logger.Printf("consume: group %s, topics %v, commit %s (%s), max-rate %.0f, control http://%s",
		c.group, c.topics, c.commitMode, c.commitInterval, c.maxRate, ln.Addr())

	// Commits périodiques en mode auto (suspendus en pause).
	if c.commitMode == "auto" {
		go func() {
			t := time.NewTicker(c.commitInterval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					c.mu.Lock()
					skip := c.paused || c.cl == nil
					c.mu.Unlock()
					if skip {
						continue
					}
					if err := c.commit(ctx); err != nil {
						logger.Printf("auto commit: %v", err)
					}
				}
			}
		}()
	}

	runErr := c.run(ctx)
	c.setState(stateStopped)
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	c.mu.Lock()
	logger.Printf("consume done: %d messages consumed", c.total)
	c.mu.Unlock()
	return runErr
}

// run enchaîne les sessions : une session s'achève sur un reset (puis on en ouvre une
// autre) ou sur l'annulation du contexte.
func (c *consumer) run(ctx context.Context) error {
	for {
		req, err := c.runSession(ctx)
		if err != nil {
			return err
		}
		if req == nil {
			return nil // contexte annulé
		}
		logger.Printf("reset requested: %s", req.spec)
		err = c.applyReset(ctx, req.spec)
		if err != nil {
			logger.Printf("reset: %v", err)
		} else {
			logger.Printf("reset applied (%s), rejoining group %s", req.spec, c.group)
		}
		req.reply <- err
		if ctx.Err() != nil {
			return nil
		}
	}
}

// runSession ouvre un client, rejoint le groupe et consomme jusqu'à une demande de
// reset (renvoyée) ou l'annulation du contexte (nil, nil).
func (c *consumer) runSession(ctx context.Context) (*resetRequest, error) {
	c.mu.Lock()
	c.session++
	session := c.session
	c.mu.Unlock()

	opts, err := c.sessionOpts(ctx, session)
	if err != nil {
		return nil, err
	}
	cl, err := newClient(ctx, c.bootstrap, opts...)
	if err != nil {
		return nil, err
	}
	defer c.closeSession()
	// À l'arrêt (contexte annulé) en mode auto : dernier commit avant de quitter le
	// groupe, sur un contexte neuf. Exécuté avant closeSession (ordre LIFO des defer).
	defer func() {
		if ctx.Err() == nil || c.commitMode != "auto" {
			return
		}
		c.mu.Lock()
		paused := c.paused
		c.mu.Unlock()
		if paused {
			return
		}
		fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.commit(fctx); err != nil {
			logger.Printf("final commit: %v", err)
		}
	}()

	// Offsets déjà commités du groupe, pour le statut. Au premier usage d'un groupe
	// le coordinateur peut être indisponible quelques secondes (__consumer_offsets
	// en cours de création) : on réessaie, puis on continue sans (non bloquant).
	committed, err := fetchCommittedRetry(ctx, kadm.NewClient(cl), c.group, c.topics, 30*time.Second)
	if err != nil {
		logger.Printf("session %d: %v (status without initial committed offsets)", session, err)
		committed = make(map[tp]int64)
	}
	c.mu.Lock()
	c.cl = cl
	c.consumed = make(map[tp]consumedOffset)
	c.committed = committed
	if c.paused {
		c.state = statePaused
		cl.PauseFetchTopics(c.topics...)
	} else {
		c.state = stateRunning
	}
	c.mu.Unlock()
	logger.Printf("session %d opened (group %s, %d partition(s) with a committed offset)", session, c.group, len(committed))

	batch := 1000
	if c.maxRate > 0 && c.maxRate < float64(batch) {
		batch = max(1, int(c.maxRate))
	}
	var next time.Time // prochain instant d'émission autorisé par --max-rate
	for {
		// Pause : on bloque sans quitter le groupe (les heartbeats continuent).
		if req, ok := c.waitRunning(ctx, cl); !ok {
			return req, nil
		}
		pctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fetches := cl.PollRecords(pctx, batch)
		cancel()
		if fetches.IsClientClosed() {
			return nil, nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return
			}
			logger.Printf("fetch %s/%d: %v", t, p, err)
		})
		for _, r := range fetches.Records() {
			if req, ok := c.waitRunning(ctx, cl); !ok {
				return req, nil
			}
			if c.maxRate > 0 {
				now := time.Now()
				if next.Before(now) {
					next = now
				}
				next = next.Add(time.Duration(float64(time.Second) / c.maxRate))
				if d := next.Sub(now); d > 0 {
					select {
					case <-ctx.Done():
						return nil, nil
					case <-time.After(d):
					}
				}
			}
			c.mu.Lock()
			// un lot tiré avant une révocation peut encore porter des records
			// d'une partition qui n'est plus à ce membre : les ignorer, sinon le
			// commit suivant réécrirait son offset par-dessus celui du membre
			// qui l'a reprise (hors --assign, où tout est à ce consumer)
			if c.assign || c.serverSide || c.assigned[tp{r.Topic, r.Partition}] {
				c.consumed[tp{r.Topic, r.Partition}] = consumedOffset{offset: r.Offset, epoch: r.LeaderEpoch}
				c.total++
			}
			c.mu.Unlock()
		}
	}
}

// sessionOpts construit les options du client d'une session : membre du
// groupe, ou consommation directe de toutes les partitions (--assign) à
// partir des offsets commités du groupe.
func (c *consumer) sessionOpts(ctx context.Context, session int) ([]kgo.Opt, error) {
	opts := []kgo.Opt{kgo.FetchMaxWait(time.Second)}
	if c.assign {
		adm, err := newClient(ctx, c.bootstrap)
		if err != nil {
			return nil, err
		}
		defer adm.Close()
		details, err := waitTopics(ctx, kadm.NewClient(adm), c.topics)
		if err != nil {
			return nil, err
		}
		committed, err := fetchCommittedRetry(ctx, kadm.NewClient(adm), c.group, c.topics, 30*time.Second)
		if err != nil {
			committed = make(map[tp]int64)
		}
		parts := make(map[string]map[int32]kgo.Offset)
		for _, td := range details {
			parts[td.Topic] = make(map[int32]kgo.Offset)
			for _, pd := range td.Partitions {
				off := kgo.NewOffset().AtStart()
				if at, ok := committed[tp{td.Topic, pd.Partition}]; ok {
					off = kgo.NewOffset().At(at)
				}
				parts[td.Topic][pd.Partition] = off
			}
		}
		return append(opts, kgo.ConsumePartitions(parts)), nil
	}
	opts = append(opts,
		kgo.ConsumerGroup(c.group),
		kgo.ConsumeTopics(c.topics...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			logger.Printf("session %d: partitions assigned %v", session, assigned)
			c.track(assigned, true)
		}),
		kgo.OnPartitionsRevoked(func(cbCtx context.Context, _ *kgo.Client, revoked map[string][]int32) {
			logger.Printf("session %d: partitions revoked %v", session, revoked)
			c.track(revoked, false)
			// pas de retenue à l'arrêt (contexte du consumer annulé)
			if c.revokeDelay > 0 && ctx.Err() == nil {
				logger.Printf("session %d: revocation held for %s", session, c.revokeDelay)
				select {
				case <-ctx.Done():
				case <-cbCtx.Done():
				case <-time.After(c.revokeDelay):
				}
			}
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			c.track(lost, false)
		}),
	)
	if c.balancer != nil {
		opts = append(opts, kgo.Balancers(c.balancer))
	}
	if c.serverSide {
		opts = append(opts, kgo.ServerSideBalancer())
	}
	if c.sessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(c.sessionTimeout), kgo.HeartbeatInterval(c.sessionTimeout/5))
	}
	if c.revokeDelay > 0 {
		opts = append(opts, kgo.RebalanceTimeout(c.revokeDelay+30*time.Second))
	}
	return opts, nil
}

// track tient à jour les partitions assignées par le groupe. Une partition
// révoquée sort aussi des offsets consommés : sinon le commit suivant
// réécrirait son ancien offset par-dessus celui du membre qui la reprend.
func (c *consumer) track(parts map[string][]int32, assigned bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for t, ps := range parts {
		for _, p := range ps {
			if assigned {
				c.assigned[tp{t, p}] = true
			} else {
				delete(c.assigned, tp{t, p})
				delete(c.consumed, tp{t, p})
			}
		}
	}
	if assigned {
		c.assignedAt = time.Now()
	}
}

// waitRunning rend la main dès que la consommation est autorisée ; renvoie false
// sur un reset (avec la demande) ou sur l'annulation du contexte (nil).
func (c *consumer) waitRunning(ctx context.Context, cl *kgo.Client) (*resetRequest, bool) {
	for {
		select {
		case <-ctx.Done():
			return nil, false
		case req := <-c.resetCh:
			return &req, false
		default:
		}
		c.mu.Lock()
		paused := c.paused
		c.mu.Unlock()
		if !paused {
			return nil, true
		}
		select {
		case <-ctx.Done():
			return nil, false
		case req := <-c.resetCh:
			return &req, false
		case <-c.wake:
		}
	}
}

// closeSession ferme le client courant (ce qui quitte le groupe).
func (c *consumer) closeSession() {
	c.mu.Lock()
	cl := c.cl
	c.cl = nil
	c.mu.Unlock()
	if cl != nil {
		cl.Close()
	}
}

func (c *consumer) setState(s string) {
	c.mu.Lock()
	c.state = s
	c.mu.Unlock()
}

// applyReset réécrit les offsets commités du groupe (vide après la fermeture de la
// session) sur toutes les partitions des topics suivis.
func (c *consumer) applyReset(ctx context.Context, spec string) error {
	c.setState(stateResetting)
	cl, err := newClient(ctx, c.bootstrap)
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	var offsets kadm.Offsets
	switch {
	case spec == "earliest":
		listed, err := adm.ListStartOffsets(ctx, c.topics...)
		if err != nil {
			return fmt.Errorf("start offsets: %w", err)
		}
		if err := listed.Error(); err != nil {
			return fmt.Errorf("start offsets: %w", err)
		}
		offsets = listed.Offsets()
	case spec == "latest":
		listed, err := adm.ListEndOffsets(ctx, c.topics...)
		if err != nil {
			return fmt.Errorf("end offsets: %w", err)
		}
		if err := listed.Error(); err != nil {
			return fmt.Errorf("end offsets: %w", err)
		}
		offsets = listed.Offsets()
	case strings.HasPrefix(spec, "offset="):
		n, err := strconv.ParseInt(strings.TrimPrefix(spec, "offset="), 10, 64)
		if err != nil || n < 0 {
			return fmt.Errorf("invalid offset in %q", spec)
		}
		details, err := adm.ListTopics(ctx, c.topics...)
		if err != nil {
			return fmt.Errorf("metadata: %w", err)
		}
		if err := details.Error(); err != nil {
			return fmt.Errorf("metadata: %w", err)
		}
		offsets = make(kadm.Offsets)
		for _, td := range details {
			for _, pd := range td.Partitions {
				offsets.AddOffset(td.Topic, pd.Partition, n, -1)
			}
		}
	default:
		return fmt.Errorf("unknown reset target %q (earliest | latest | offset=<n>)", spec)
	}

	// Le départ du groupe vient d'être envoyé ; on réessaie tant que le broker
	// considère le groupe non vide.
	deadline := time.Now().Add(15 * time.Second)
	for {
		resps, err := adm.CommitOffsets(ctx, c.group, offsets)
		if err == nil {
			err = resps.Error()
		}
		if err == nil {
			c.mu.Lock()
			c.consumed = make(map[tp]consumedOffset)
			c.committed = make(map[tp]int64)
			resps.Each(func(r kadm.OffsetResponse) {
				c.committed[tp{r.Topic, r.Partition}] = r.At
			})
			c.lastCommitAt = time.Now()
			c.lastCommitErr = ""
			c.mu.Unlock()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("committing reset offsets: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// commit envoie au broker les offsets consommés (dernier + 1) de la session courante.
func (c *consumer) commit(ctx context.Context) error {
	c.mu.Lock()
	cl := c.cl
	if cl == nil {
		c.mu.Unlock()
		return errors.New("no active session")
	}
	toCommit := make(map[string]map[int32]kgo.EpochOffset, len(c.consumed))
	for k, v := range c.consumed {
		if toCommit[k.topic] == nil {
			toCommit[k.topic] = make(map[int32]kgo.EpochOffset)
		}
		toCommit[k.topic][k.partition] = kgo.EpochOffset{Epoch: v.epoch, Offset: v.offset + 1}
	}
	c.mu.Unlock()
	if len(toCommit) == 0 {
		return nil
	}
	if c.assign {
		return c.commitAssigned(ctx, cl, toCommit)
	}

	var cerr error
	cl.CommitOffsetsSync(ctx, toCommit, func(_ *kgo.Client, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			cerr = err
			return
		}
		requested := make(map[tp]int64)
		for _, t := range req.Topics {
			for _, p := range t.Partitions {
				requested[tp{t.Topic, p.Partition}] = p.Offset
			}
		}
		var failed []string
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				k := tp{t.Topic, p.Partition}
				if e := kerr.ErrorForCode(p.ErrorCode); e != nil {
					failed = append(failed, fmt.Sprintf("%s/%d: %v", k.topic, k.partition, e))
					continue
				}
				if off, ok := requested[k]; ok {
					c.committed[k] = off
				}
			}
		}
		if len(failed) > 0 {
			cerr = errors.New(strings.Join(failed, "; "))
		}
	})
	c.mu.Lock()
	c.lastCommitAt = time.Now()
	if cerr != nil {
		c.lastCommitErr = cerr.Error()
	} else {
		c.lastCommitErr = ""
	}
	c.mu.Unlock()
	return cerr
}

// commitAssigned commite hors adhésion au groupe (--assign), comme un client
// assign() : OffsetCommit sans génération ni membre.
func (c *consumer) commitAssigned(ctx context.Context, cl *kgo.Client, toCommit map[string]map[int32]kgo.EpochOffset) error {
	offsets := make(kadm.Offsets)
	for t, parts := range toCommit {
		for p, eo := range parts {
			offsets.AddOffset(t, p, eo.Offset, eo.Epoch)
		}
	}
	resps, err := kadm.NewClient(cl).CommitOffsets(ctx, c.group, offsets)
	if err == nil {
		err = resps.Error()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastCommitAt = time.Now()
	c.lastCommitErr = ""
	if err != nil {
		c.lastCommitErr = err.Error()
		return err
	}
	resps.Each(func(r kadm.OffsetResponse) {
		c.committed[tp{r.Topic, r.Partition}] = r.At
	})
	return nil
}

// fetchCommittedRetry réessaie fetchCommitted jusqu'à l'échéance.
func fetchCommittedRetry(ctx context.Context, adm *kadm.Client, group string, topics []string, timeout time.Duration) (map[tp]int64, error) {
	deadline := time.Now().Add(timeout)
	for {
		out, err := fetchCommitted(ctx, adm, group, topics)
		if err == nil || time.Now().After(deadline) {
			return out, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// fetchCommitted lit les offsets commités du groupe pour les topics suivis.
func fetchCommitted(ctx context.Context, adm *kadm.Client, group string, topics []string) (map[tp]int64, error) {
	resps, err := adm.FetchOffsetsForTopics(ctx, group, topics...)
	if err != nil {
		return nil, fmt.Errorf("committed offsets of group %s: %w", group, err)
	}
	if err := resps.Error(); err != nil {
		return nil, fmt.Errorf("committed offsets of group %s: %w", group, err)
	}
	out := make(map[tp]int64)
	resps.Each(func(r kadm.OffsetResponse) {
		if r.At >= 0 {
			out[tp{r.Topic, r.Partition}] = r.At
		}
	})
	return out, nil
}

// --- API HTTP de pilotage ---

type partitionStatus struct {
	Topic              string `json:"topic"`
	Partition          int32  `json:"partition"`
	LastConsumedOffset int64  `json:"last_consumed_offset"` // -1 si rien consommé dans cette session
	CommittedOffset    int64  `json:"committed_offset"`     // -1 si aucun commit connu
}

type status struct {
	State          string            `json:"state"`
	Group          string            `json:"group"`
	Topics         []string          `json:"topics"`
	CommitMode     string            `json:"commit_mode"`
	CommitInterval string            `json:"commit_interval"`
	MaxRate        float64           `json:"max_rate"`
	Session        int               `json:"session"`
	ConsumedTotal  int64             `json:"consumed_total"`
	LastCommitAt   *time.Time        `json:"last_commit_at,omitempty"`
	LastCommitErr  string            `json:"last_commit_error,omitempty"`
	Partitions     []partitionStatus `json:"partitions"`
	MemberID       string            `json:"member_id,omitempty"`   // membre du groupe (vide en --assign)
	Generation     int32             `json:"generation"`            // génération connue du membre, -1 hors groupe
	Assigned       []string          `json:"assigned"`              // partitions assignées, « topic/p »
	AssignedAt     *time.Time        `json:"assigned_at,omitempty"` // dernière assignation reçue
}

func (c *consumer) snapshot() status {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := status{
		State:          c.state,
		Group:          c.group,
		Topics:         c.topics,
		CommitMode:     c.commitMode,
		CommitInterval: c.commitInterval.String(),
		MaxRate:        c.maxRate,
		Session:        c.session,
		ConsumedTotal:  c.total,
		LastCommitErr:  c.lastCommitErr,
		Partitions:     []partitionStatus{},
	}
	if c.paused && c.state == stateRunning {
		st.State = statePaused
	}
	if !c.lastCommitAt.IsZero() {
		t := c.lastCommitAt
		st.LastCommitAt = &t
	}
	st.Generation = -1
	if c.cl != nil && !c.assign {
		st.MemberID, st.Generation = c.cl.GroupMetadata()
	}
	st.Assigned = []string{}
	for k := range c.assigned {
		st.Assigned = append(st.Assigned, fmt.Sprintf("%s/%d", k.topic, k.partition))
	}
	sort.Strings(st.Assigned)
	if !c.assignedAt.IsZero() {
		t := c.assignedAt
		st.AssignedAt = &t
	}
	keys := make(map[tp]struct{})
	for k := range c.consumed {
		keys[k] = struct{}{}
	}
	for k := range c.committed {
		keys[k] = struct{}{}
	}
	for k := range keys {
		ps := partitionStatus{Topic: k.topic, Partition: k.partition, LastConsumedOffset: -1, CommittedOffset: -1}
		if v, ok := c.consumed[k]; ok {
			ps.LastConsumedOffset = v.offset
		}
		if v, ok := c.committed[k]; ok {
			ps.CommittedOffset = v
		}
		st.Partitions = append(st.Partitions, ps)
	}
	sort.Slice(st.Partitions, func(i, j int) bool {
		a, b := st.Partitions[i], st.Partitions[j]
		if a.Topic != b.Topic {
			return a.Topic < b.Topic
		}
		return a.Partition < b.Partition
	})
	return st
}

func (c *consumer) handler(ctx context.Context) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, c.snapshot())
	})
	mux.HandleFunc("POST /pause", func(w http.ResponseWriter, r *http.Request) {
		c.setPaused(true)
		logger.Printf("control: pause")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "state": c.snapshot().State})
	})
	mux.HandleFunc("POST /resume", func(w http.ResponseWriter, r *http.Request) {
		c.setPaused(false)
		logger.Printf("control: resume")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "state": c.snapshot().State})
	})
	mux.HandleFunc("POST /commit", func(w http.ResponseWriter, r *http.Request) {
		cctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := c.commit(cctx); err != nil {
			logger.Printf("control: commit: %v", err)
			writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		logger.Printf("control: commit")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "partitions": c.snapshot().Partitions})
	})
	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, r *http.Request) {
		spec := r.URL.Query().Get("to")
		if !validResetSpec(spec) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "parameter to required: earliest | latest | offset=<n>"})
			return
		}
		req := resetRequest{spec: spec, reply: make(chan error, 1)}
		select {
		case c.resetCh <- req:
		case <-ctx.Done():
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": "shutting down"})
			return
		case <-time.After(10 * time.Second):
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": "consume loop unavailable (reset already in progress?)"})
			return
		}
		select {
		case err := <-req.reply:
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"ok": true, "partitions": c.snapshot().Partitions})
		case <-time.After(60 * time.Second):
			writeJSON(w, http.StatusGatewayTimeout, map[string]any{"ok": false, "error": "reset timed out"})
		}
	})
	return mux
}

// validResetSpec accepte earliest, latest ou offset=<entier >= 0>.
func validResetSpec(spec string) bool {
	if spec == "earliest" || spec == "latest" {
		return true
	}
	if !strings.HasPrefix(spec, "offset=") {
		return false
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(spec, "offset="), 10, 64)
	return err == nil && n >= 0
}

func (c *consumer) setPaused(p bool) {
	c.mu.Lock()
	c.paused = p
	cl := c.cl
	if cl != nil && c.state != stateResetting {
		if p {
			cl.PauseFetchTopics(c.topics...)
			c.state = statePaused
		} else {
			cl.ResumeFetchTopics(c.topics...)
			c.state = stateRunning
		}
	}
	c.mu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
