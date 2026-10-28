// Package sink définit la sortie des documents et une sortie stdout.
package sink

import (
	"bufio"
	"encoding/json"
	"io"
	"sync"
)

// Doc est un document à publier dans un data stream (lag, metrics, health).
type Doc struct {
	Stream string
	Body   map[string]any
}

// Sink publie des documents.
type Sink interface {
	Emit(docs []Doc) error
}

// Stdout écrit un objet JSON par ligne, avec le nom du flux dans _stream.
type Stdout struct {
	mu sync.Mutex
	w  *bufio.Writer
}

// NewStdout crée une sortie texte.
func NewStdout(w io.Writer) *Stdout {
	return &Stdout{w: bufio.NewWriter(w)}
}

// Emit écrit les documents.
func (s *Stdout) Emit(docs []Doc) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	enc := json.NewEncoder(s.w)
	for _, d := range docs {
		d.Body["_stream"] = d.Stream
		if err := enc.Encode(d.Body); err != nil {
			return err
		}
	}
	return s.w.Flush()
}
