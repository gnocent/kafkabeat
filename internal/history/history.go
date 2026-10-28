// Package history conserve, par partition, l'historique des offsets de fin
// (LEO) observés, avec plusieurs paliers de granularité, et en déduit un
// encadrement de l'âge d'un offset sans aucune hypothèse de débit.
//
// Chaque entrée signifie : « le LEO valait Leo de First à Last » (ms Unix).
// Pour un offset c tel que Leo_i <= c < Leo_{i+1}, le message c a été ajouté
// au journal entre Last_i (exclu) et First_{i+1} (inclus). Supprimer des
// entrées ne fait qu'élargir l'encadrement : la compaction est toujours sûre.
package history

import (
	"sort"
	"time"
)

// Key identifie une partition.
type Key struct {
	Topic     string
	Partition int32
}

// Entry : LEO observé égal à Leo de First à Last (millisecondes Unix).
type Entry struct {
	Leo   int64
	First int64
	Last  int64
}

// Tier : jusqu'à Span d'âge (et au-delà du palier précédent), les entrées
// sont au plus une tous les Step ; les paliers sont croissants.
type Tier struct {
	Step time.Duration
	Span time.Duration
}

// Bracket est l'encadrement de l'âge d'un offset.
type Bracket struct {
	// Known est faux quand la partition n'a aucune observation.
	Known bool
	// Lower est l'âge minimal (le message existait déjà à now-Lower).
	Lower time.Duration
	// Upper est l'âge maximal ; UpperKnown est faux quand l'offset est
	// antérieur à la plus ancienne observation (encadrement ouvert).
	Upper      time.Duration
	UpperKnown bool
}

// Method qualifie l'encadrement pour les documents.
func (b Bracket) Method() string {
	switch {
	case !b.Known:
		return "none"
	case !b.UpperKnown:
		return "bracket_open"
	default:
		return "bracket"
	}
}

// Store est l'ensemble des séries.
type Store struct {
	tiers  []Tier
	series map[Key]*series
}

type series struct {
	entries []Entry
}

// New crée un magasin vide avec les paliers donnés (au moins un).
func New(tiers []Tier) *Store {
	return &Store{tiers: tiers, series: make(map[Key]*series)}
}

// Observe enregistre le LEO d'une partition à l'instant now. Elle renvoie
// vrai si le LEO a reculé (troncature ou recréation) : la série repart alors
// de zéro, car les anciennes entrées ne décrivent plus le journal.
func (s *Store) Observe(key Key, leo int64, now time.Time) (truncated bool) {
	t := now.UnixMilli()
	sr := s.series[key]
	if sr == nil {
		sr = &series{}
		s.series[key] = sr
	}
	n := len(sr.entries)
	if n == 0 {
		sr.entries = append(sr.entries, Entry{Leo: leo, First: t, Last: t})
		return false
	}
	last := &sr.entries[n-1]
	if t < last.Last {
		t = last.Last // horloge revenue en arrière : on n'écrit jamais dans le passé
	}
	switch {
	case leo == last.Leo:
		last.Last = t
	case leo > last.Leo:
		sr.entries = append(sr.entries, Entry{Leo: leo, First: t, Last: t})
	default:
		sr.entries = append(sr.entries[:0], Entry{Leo: leo, First: t, Last: t})
		return true
	}
	return false
}

// Bracket encadre l'âge, à l'instant now, du message situé à offset.
// Un offset égal ou postérieur au dernier LEO observé a un âge nul.
func (s *Store) Bracket(key Key, offset int64, now time.Time) Bracket {
	sr := s.series[key]
	if sr == nil || len(sr.entries) == 0 {
		return Bracket{}
	}
	e := sr.entries
	n := len(e)
	if offset >= e[n-1].Leo {
		return Bracket{Known: true, UpperKnown: true}
	}
	t := now.UnixMilli()
	// i = plus grand indice tel que e[i].Leo <= offset ; -1 si aucun.
	i := sort.Search(n, func(k int) bool { return e[k].Leo > offset }) - 1
	if i < 0 {
		return Bracket{Known: true, Lower: ms(t - e[0].First)}
	}
	return Bracket{
		Known:      true,
		Lower:      ms(t - e[i+1].First),
		Upper:      ms(t - e[i].Last),
		UpperKnown: true,
	}
}

func ms(d int64) time.Duration {
	if d < 0 {
		return 0
	}
	return time.Duration(d) * time.Millisecond
}

// Compact applique les paliers à toutes les séries : au-delà de l'horizon du
// dernier palier les entrées sont supprimées ; dans chaque palier, deux
// entrées conservées sont espacées d'au moins Step. La dernière entrée d'une
// série (le plateau courant) est toujours conservée.
func (s *Store) Compact(now time.Time) {
	t := now.UnixMilli()
	for key, sr := range s.series {
		sr.entries = compact(sr.entries, s.tiers, t)
		if len(sr.entries) == 0 {
			delete(s.series, key)
		}
	}
}

func compact(e []Entry, tiers []Tier, now int64) []Entry {
	n := len(e)
	if n <= 1 {
		return e
	}
	horizon := tiers[len(tiers)-1].Span.Milliseconds()
	out := e[:0]
	var lastKept *Entry
	for k := 0; k < n; k++ {
		age := now - e[k].Last
		if k == n-1 {
			out = append(out, e[k])
			break
		}
		if age > horizon {
			continue
		}
		// Tolérance de 10 % : des observations espacées d'un pas (à la gigue
		// près) doivent toutes être conservées, sinon une sur deux disparaît.
		step := tierStep(tiers, age)
		if lastKept != nil && e[k].First-lastKept.First < step-step/10 {
			continue
		}
		out = append(out, e[k])
		lastKept = &out[len(out)-1]
	}
	return out
}

// tierStep renvoie le pas du palier dans lequel tombe un âge.
func tierStep(tiers []Tier, age int64) int64 {
	for _, t := range tiers {
		if age <= t.Span.Milliseconds() {
			return t.Step.Milliseconds()
		}
	}
	return tiers[len(tiers)-1].Step.Milliseconds()
}

// Forget supprime la série d'une partition disparue.
func (s *Store) Forget(key Key) {
	delete(s.series, key)
}

// Keys renvoie les partitions connues.
func (s *Store) Keys() []Key {
	keys := make([]Key, 0, len(s.series))
	for k := range s.series {
		keys = append(keys, k)
	}
	return keys
}

// LastChange renvoie l'instant où le LEO d'une partition a pris sa valeur
// courante, à un intervalle d'observation près. changed est faux si la série
// n'a vu aucune avance : at est alors le début de l'observation, et l'âge du
// dernier message n'en est qu'un minorant.
func (s *Store) LastChange(key Key) (at time.Time, changed, ok bool) {
	sr := s.series[key]
	if sr == nil || len(sr.entries) == 0 {
		return time.Time{}, false, false
	}
	n := len(sr.entries)
	return time.UnixMilli(sr.entries[n-1].First), n > 1, true
}

// Entries renvoie une copie des entrées d'une partition (tests, diagnostic).
func (s *Store) Entries(key Key) []Entry {
	sr := s.series[key]
	if sr == nil {
		return nil
	}
	return append([]Entry(nil), sr.entries...)
}

// Stats renvoie le nombre de séries et d'entrées.
func (s *Store) Stats() (series, entries int) {
	for _, sr := range s.series {
		entries += len(sr.entries)
	}
	return len(s.series), entries
}
