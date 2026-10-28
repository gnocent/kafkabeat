package collector

import (
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// pairState est l'état persistant d'un couple topic + groupe.
type pairState struct {
	Parts        map[int32]*partCommit
	LastProduced int64
	LastConsumed int64
	LastAt       time.Time // zéro : pas de référence, taux non calculables
	Window       []rateSample
	Flags        map[string]int // flag -> cycles restants
	Gone         int            // cycles restants d'émission « gone » ; 0 = vivant
}

// partCommit suit l'offset commité d'une partition : ChangedAt est l'heure de
// sa dernière variation (celle du commit quand la source la connaît, sinon
// celle du cycle) et décide de stalled ; CommittedAt est l'heure du dernier
// commit vu, même au même offset, quand la source la connaît (flux), zéro
// sinon (polling).
type partCommit struct {
	Offset      int64
	ChangedAt   time.Time
	CommittedAt time.Time
}

type rateSample struct {
	Consumed int64
	Dt       time.Duration
}

type topicState struct {
	ID string
}

// Noms des flags (documents et état).
const (
	flagOffsetReset  = "offset_reset_suspected"
	flagLogTruncated = "log_truncated_suspected"
	flagTopicRecr    = "topic_recreated"
	flagGroupRecr    = "group_recreated"
	flagResumedGap   = "resumed_after_gap"
	flagCycleIncompl = "cycle_incomplete"
	// la rétention a dépassé le commit d'une partition : offsets perdus
	flagLogStartPassed = "commit_before_log_start"
)

// flags tenus par paire ; cycle_incomplete est propre au cycle (voir flags)
var flagNames = []string{flagOffsetReset, flagLogTruncated, flagTopicRecr, flagGroupRecr, flagResumedGap, flagLogStartPassed}

func (p *pairState) raise(flag string, cycles int) {
	if p.Flags == nil {
		p.Flags = make(map[string]int)
	}
	if p.Flags[flag] < cycles {
		p.Flags[flag] = cycles
	}
}

// resetReferences oublie les références des taux après un retour arrière.
func (p *pairState) resetReferences() {
	p.LastAt = time.Time{}
	p.Window = p.Window[:0]
}

type snapshotV1 struct {
	SavedAt time.Time
	Cycle   uint64
	Pairs   map[pairKey]*pairState
	Topics  map[string]*topicState
}

const (
	stateFile   = "state.gob"
	historyFile = "history.snap"
)

// Save écrit l'état et l'historique dans dir, de façon atomique.
func (c *Collector) Save(dir string, now time.Time) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	snap := snapshotV1{SavedAt: now, Cycle: c.cycle, Pairs: c.pairs, Topics: c.topics}
	if err := writeAtomic(filepath.Join(dir, stateFile), func(f *os.File) error {
		return gob.NewEncoder(f).Encode(&snap)
	}); err != nil {
		return fmt.Errorf("state: %w", err)
	}
	if err := writeAtomic(filepath.Join(dir, historyFile), func(f *os.File) error {
		_, err := c.hist.WriteTo(f)
		return err
	}); err != nil {
		return fmt.Errorf("history: %w", err)
	}
	c.lastSnapshot = now
	return nil
}

// Load recharge l'état et l'historique depuis dir. Sans fichier, l'agent
// part de zéro. Si l'arrêt a dépassé deux cycles, le flag resumed_after_gap
// est levé sur toutes les paires et les taux repartent sans référence.
func (c *Collector) Load(dir string, now time.Time) (time.Time, error) {
	f, err := os.Open(filepath.Join(dir, stateFile))
	if errors.Is(err, os.ErrNotExist) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	defer f.Close()
	var snap snapshotV1
	if err := gob.NewDecoder(f).Decode(&snap); err != nil {
		return time.Time{}, fmt.Errorf("state: %w", err)
	}
	h, err := os.Open(filepath.Join(dir, historyFile))
	if err == nil {
		defer h.Close()
		if _, err := c.hist.ReadFrom(h); err != nil {
			return time.Time{}, fmt.Errorf("history: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return time.Time{}, err
	}
	c.cycle = snap.Cycle
	c.pairs = snap.Pairs
	c.topics = snap.Topics
	if c.pairs == nil {
		c.pairs = make(map[pairKey]*pairState)
	}
	if c.topics == nil {
		c.topics = make(map[string]*topicState)
	}
	if now.Sub(snap.SavedAt) > 2*c.cfg.Collect.Interval {
		for _, p := range c.pairs {
			p.raise(flagResumedGap, c.cfg.Collect.FlagHoldCycles)
			p.resetReferences()
		}
	}
	c.lastSnapshot = now
	return snap.SavedAt, nil
}

func writeAtomic(path string, write func(*os.File) error) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	if err := write(f); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}
