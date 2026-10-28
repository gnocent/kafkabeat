// Package journal écrit et relit le journal de production du harnais : une ligne
// JSON par message produit, avec l'heure d'envoi côté harnais. Les tests de recette
// s'en servent pour connaître le vrai lag temporel d'un offset commité.
package journal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Entry est une ligne du journal.
type Entry struct {
	Topic      string    `json:"topic"`
	Partition  int32     `json:"partition"`
	Offset     int64     `json:"offset"`
	ProducedAt time.Time `json:"produced_at"` // RFC3339Nano, heure d'envoi côté harnais
}

// Writer écrit des entrées en mode bufferisé, sûr pour un usage concurrent.
type Writer struct {
	mu  sync.Mutex
	f   *os.File
	buf *bufio.Writer
}

// NewWriter ouvre (ou crée, en ajout) le fichier de journal.
func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening journal %s: %w", path, err)
	}
	return &Writer{f: f, buf: bufio.NewWriterSize(f, 256<<10)}, nil
}

// Write ajoute une entrée au tampon.
func (w *Writer) Write(e Entry) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.buf.Write(b); err != nil {
		return err
	}
	return w.buf.WriteByte('\n')
}

// Flush vide le tampon sur le disque.
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Flush()
}

// Close vide le tampon et ferme le fichier.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	ferr := w.buf.Flush()
	cerr := w.f.Close()
	if ferr != nil {
		return ferr
	}
	return cerr
}

type key struct {
	topic     string
	partition int32
	offset    int64
}

// Reader charge un journal complet en mémoire et répond aux recherches par offset.
type Reader struct {
	byKey map[key]time.Time
}

// Open lit tout le fichier en mémoire.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening journal %s: %w", path, err)
	}
	defer f.Close()
	return Read(f)
}

// Read charge toutes les lignes d'un flux en mémoire. Les lignes vides sont ignorées.
func Read(r io.Reader) (*Reader, error) {
	rd := &Reader{byKey: make(map[key]time.Time)}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	line := 0
	for sc.Scan() {
		line++
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, fmt.Errorf("journal line %d: %w", line, err)
		}
		rd.byKey[key{e.Topic, e.Partition, e.Offset}] = e.ProducedAt
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading journal: %w", err)
	}
	return rd, nil
}

// ProducedAt renvoie l'heure d'envoi d'un offset, et false s'il est inconnu du journal.
func (r *Reader) ProducedAt(topic string, partition int32, offset int64) (time.Time, bool) {
	t, ok := r.byKey[key{topic, partition, offset}]
	return t, ok
}
