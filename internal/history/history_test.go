package history

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

var tiers = []Tier{
	{Step: 10 * time.Second, Span: 15 * time.Minute},
	{Step: time.Minute, Span: 3 * time.Hour},
	{Step: 10 * time.Minute, Span: 7 * 24 * time.Hour},
}

var t0 = time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)

func at(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }

var k = Key{Topic: "t", Partition: 0}

func TestBracketBasic(t *testing.T) {
	s := New(tiers)
	s.Observe(k, 10, at(0))
	s.Observe(k, 10, at(10))
	s.Observe(k, 20, at(20))
	s.Observe(k, 20, at(30))
	b := s.Bracket(k, 15, at(30))
	if !b.Known || !b.UpperKnown || b.Lower != 10*time.Second || b.Upper != 20*time.Second {
		t.Fatalf("offset 15 : %+v, attendu [10s, 20s]", b)
	}
	if b := s.Bracket(k, 20, at(30)); b.Lower != 0 || b.Upper != 0 || !b.UpperKnown {
		t.Fatalf("offset égal au LEO : %+v, attendu 0", b)
	}
	if b := s.Bracket(k, 5, at(30)); !b.Known || b.UpperKnown || b.Lower != 30*time.Second {
		t.Fatalf("offset antérieur à l'historique : %+v, attendu borne basse 30s et borne haute inconnue", b)
	}
	if b := s.Bracket(Key{Topic: "x"}, 0, at(30)); b.Known {
		t.Fatal("partition inconnue : l'encadrement doit être inconnu")
	}
}

// Après un long silence, la borne haute ne doit pas englober le silence :
// le LEO a été observé inchangé jusqu'au cycle précédent la rafale.
func TestBracketAfterSilence(t *testing.T) {
	s := New(tiers)
	for sec := 0; sec <= 3600; sec += 10 {
		s.Observe(k, 0, at(sec))
	}
	s.Observe(k, 100, at(3610))
	b := s.Bracket(k, 50, at(3610))
	if b.Lower != 0 || b.Upper != 10*time.Second {
		t.Fatalf("après silence : %+v, attendu [0, 10s]", b)
	}
	if n := len(s.Entries(k)); n != 2 {
		t.Fatalf("un plateau doit tenir en une entrée : %d entrées", n)
	}
}

func TestTruncation(t *testing.T) {
	s := New(tiers)
	s.Observe(k, 100, at(0))
	if s.Observe(k, 150, at(10)) {
		t.Fatal("une hausse du LEO n'est pas une troncature")
	}
	if !s.Observe(k, 20, at(20)) {
		t.Fatal("un recul du LEO doit être signalé")
	}
	if e := s.Entries(k); len(e) != 1 || e[0].Leo != 20 {
		t.Fatalf("la série doit repartir de zéro : %+v", e)
	}
}

func TestLastChange(t *testing.T) {
	s := New(tiers)
	if _, _, ok := s.LastChange(k); ok {
		t.Fatal("partition inconnue : aucun instant attendu")
	}
	s.Observe(k, 10, at(0))
	s.Observe(k, 10, at(10))
	if a, changed, ok := s.LastChange(k); !ok || changed || !a.Equal(at(0)) {
		t.Fatalf("aucune avance vue : début d'observation attendu, %v %v %v", a, changed, ok)
	}
	s.Observe(k, 20, at(20))
	s.Observe(k, 20, at(30))
	if a, changed, _ := s.LastChange(k); !changed || !a.Equal(at(20)) {
		t.Fatalf("avance à 20 s attendue : %v %v", a, changed)
	}
}

func TestClockGoingBack(t *testing.T) {
	s := New(tiers)
	s.Observe(k, 1, at(100))
	s.Observe(k, 2, at(90))
	e := s.Entries(k)
	if e[1].First < e[0].Last {
		t.Fatalf("les entrées doivent rester ordonnées : %+v", e)
	}
}

// La compaction amincit les entrées anciennes et laisse les encadrements
// valides : la vérité reste entre les bornes, qui ne font que s'élargir.
func TestCompactKeepsBracketsValid(t *testing.T) {
	full := New(tiers)
	compacted := New(tiers)
	rng := rand.New(rand.NewSource(1))
	type produced struct {
		offset int64
		sec    int
	}
	var truth []produced
	leo := int64(0)
	end := 4 * 3600
	for sec := 0; sec <= end; sec += 10 {
		// débit irrégulier : rafales, silences
		var n int64
		switch r := rng.Intn(10); {
		case r < 4:
			n = 0
		case r < 9:
			n = int64(rng.Intn(50))
		default:
			n = int64(1000 + rng.Intn(5000))
		}
		for i := int64(0); i < n; i++ {
			truth = append(truth, produced{offset: leo + i, sec: sec})
		}
		leo += n
		full.Observe(k, leo, at(sec))
		compacted.Observe(k, leo, at(sec))
		if sec%60 == 0 {
			compacted.Compact(at(sec))
		}
	}
	now := at(end)
	nf := len(full.Entries(k))
	nc := len(compacted.Entries(k))
	if nc >= nf || nc > 400 {
		t.Fatalf("compaction inefficace : %d entrées contre %d", nc, nf)
	}
	for i := 0; i < 2000; i++ {
		p := truth[rng.Intn(len(truth))]
		age := now.Sub(at(p.sec))
		for name, s := range map[string]*Store{"full": full, "compacted": compacted} {
			b := s.Bracket(k, p.offset, now)
			if !b.Known || !b.UpperKnown {
				t.Fatalf("%s : encadrement inconnu pour %+v", name, p)
			}
			// le message a été ajouté dans (Last_i, First_{i+1}] : produit à sec,
			// observé à sec, donc âge réel = age, borne basse <= age < borne haute + 10s.
			if b.Lower > age || b.Upper < age {
				t.Fatalf("%s : vérité %s hors de [%s, %s] pour %+v", name, age, b.Lower, b.Upper, p)
			}
		}
		bf := full.Bracket(k, p.offset, now)
		bc := compacted.Bracket(k, p.offset, now)
		if bc.Lower > bf.Lower || bc.Upper < bf.Upper {
			t.Fatalf("la compaction doit seulement élargir : %+v contre %+v", bc, bf)
		}
		if age <= 14*time.Minute && bf.Upper-bf.Lower > 10*time.Second {
			t.Fatalf("dans la fenêtre fine, largeur %s > 10s", bf.Upper-bf.Lower)
		}
		if age <= 14*time.Minute && bc.Upper-bc.Lower > 10*time.Second {
			t.Fatalf("compacté, fenêtre fine, largeur %s > 10s", bc.Upper-bc.Lower)
		}
	}
	// Au-delà de l'horizon, tout disparaît sauf le plateau courant.
	compacted.Compact(at(end + 8*24*3600))
	if n := len(compacted.Entries(k)); n != 1 {
		t.Fatalf("au-delà de l'horizon il doit rester 1 entrée, pas %d", n)
	}
}

// Des observations au pas du premier palier, avec une gigue de quelques
// millisecondes, doivent toutes survivre à la compaction : sinon la largeur
// de l'encadrement double.
func TestCompactKeepsJitteredObservations(t *testing.T) {
	s := New(tiers)
	rng := rand.New(rand.NewSource(3))
	var n int
	for sec := 0; sec < 600; sec += 10 {
		jitter := time.Duration(rng.Intn(100)-50) * time.Millisecond
		s.Observe(k, int64(sec), at(sec).Add(jitter))
		n++
	}
	s.Compact(at(600))
	if got := len(s.Entries(k)); got != n {
		t.Fatalf("%d entrées conservées sur %d observations", got, n)
	}
	b := s.Bracket(k, 305, at(600))
	if b.Upper-b.Lower > 10*time.Second+100*time.Millisecond {
		t.Fatalf("largeur %s > 10 s", b.Upper-b.Lower)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	s := New(tiers)
	rng := rand.New(rand.NewSource(2))
	for p := int32(0); p < 5; p++ {
		key := Key{Topic: "topic-é", Partition: p}
		leo := int64(rng.Intn(1000))
		for sec := 0; sec < 600; sec += 10 {
			if rng.Intn(3) > 0 {
				leo += int64(rng.Intn(100))
			}
			s.Observe(key, leo, at(sec))
		}
	}
	var buf bytes.Buffer
	if _, err := s.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	r := New(tiers)
	if _, err := r.ReadFrom(&buf); err != nil {
		t.Fatal(err)
	}
	for _, key := range s.Keys() {
		if !reflect.DeepEqual(s.Entries(key), r.Entries(key)) {
			t.Fatalf("série %v différente après relecture", key)
		}
	}
	if _, err := New(tiers).ReadFrom(bytes.NewReader([]byte("XXXX"))); err == nil {
		t.Fatal("un format inconnu doit être refusé")
	}
}

func TestSnapshotCorruptedCounts(t *testing.T) {
	// compteurs démesurés lus d'un fichier corrompu : une erreur de lecture,
	// ni panique ni allocation géante
	huge := binary.AppendUvarint(nil, 1<<62)
	cases := map[string][]byte{
		"séries":  append([]byte("KBH1"), huge...),
		"entrées": append(append([]byte("KBH1"), 1, 1, 't', 0), huge...),
	}
	for name, data := range cases {
		if _, err := New(tiers).ReadFrom(bytes.NewReader(data)); err == nil {
			t.Errorf("%s : un snapshot tronqué doit être refusé", name)
		}
	}
}

func BenchmarkObserveAndCompact(b *testing.B) {
	s := New(tiers)
	keys := make([]Key, 30000)
	for i := range keys {
		keys[i] = Key{Topic: "t", Partition: int32(i)}
	}
	now := t0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		now = now.Add(10 * time.Second)
		for j, k := range keys {
			s.Observe(k, int64(i*10+j%3), now)
		}
		if i%6 == 0 {
			s.Compact(now)
		}
	}
}
