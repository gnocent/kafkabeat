package history

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
)

// Format binaire compact : en-tête "KBH1", puis pour chaque série :
// topic (longueur + octets), partition, nombre d'entrées, et les entrées en
// deltas varint (leo, first, last) par rapport à l'entrée précédente.
var magic = []byte("KBH1")

// WriteTo sérialise le magasin.
func (s *Store) WriteTo(w io.Writer) (int64, error) {
	cw := &countWriter{w: bufio.NewWriterSize(w, 1<<20)}
	if _, err := cw.Write(magic); err != nil {
		return cw.n, err
	}
	keys := s.Keys()
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Topic != keys[j].Topic {
			return keys[i].Topic < keys[j].Topic
		}
		return keys[i].Partition < keys[j].Partition
	})
	buf := make([]byte, binary.MaxVarintLen64)
	putU := func(v uint64) error {
		n := binary.PutUvarint(buf, v)
		_, err := cw.Write(buf[:n])
		return err
	}
	putI := func(v int64) error {
		n := binary.PutVarint(buf, v)
		_, err := cw.Write(buf[:n])
		return err
	}
	if err := putU(uint64(len(keys))); err != nil {
		return cw.n, err
	}
	for _, k := range keys {
		e := s.series[k].entries
		if err := putU(uint64(len(k.Topic))); err != nil {
			return cw.n, err
		}
		if _, err := cw.Write([]byte(k.Topic)); err != nil {
			return cw.n, err
		}
		if err := putI(int64(k.Partition)); err != nil {
			return cw.n, err
		}
		if err := putU(uint64(len(e))); err != nil {
			return cw.n, err
		}
		var prev Entry
		for _, en := range e {
			if err := putI(en.Leo - prev.Leo); err != nil {
				return cw.n, err
			}
			if err := putI(en.First - prev.Last); err != nil {
				return cw.n, err
			}
			if err := putI(en.Last - en.First); err != nil {
				return cw.n, err
			}
			prev = en
		}
	}
	return cw.n, cw.w.(*bufio.Writer).Flush()
}

// ReadFrom remplace le contenu du magasin par celui du flux.
func (s *Store) ReadFrom(r io.Reader) (int64, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	cr := &countReader{r: br}
	head := make([]byte, len(magic))
	if _, err := io.ReadFull(cr, head); err != nil {
		return cr.n, err
	}
	if string(head) != string(magic) {
		return cr.n, errors.New("history snapshot: unknown format")
	}
	nSeries, err := binary.ReadUvarint(cr)
	if err != nil {
		return cr.n, err
	}
	loaded := make(map[Key]*series, min(nSeries, maxSeriesHint))
	for i := uint64(0); i < nSeries; i++ {
		tl, err := binary.ReadUvarint(cr)
		if err != nil {
			return cr.n, err
		}
		if tl > 1<<16 {
			return cr.n, fmt.Errorf("history snapshot: topic name too long (%d)", tl)
		}
		topic := make([]byte, tl)
		if _, err := io.ReadFull(cr, topic); err != nil {
			return cr.n, err
		}
		part, err := binary.ReadVarint(cr)
		if err != nil {
			return cr.n, err
		}
		ne, err := binary.ReadUvarint(cr)
		if err != nil {
			return cr.n, err
		}
		entries := make([]Entry, 0, min(ne, maxEntriesHint))
		var prev Entry
		for j := uint64(0); j < ne; j++ {
			dl, err := binary.ReadVarint(cr)
			if err != nil {
				return cr.n, err
			}
			df, err := binary.ReadVarint(cr)
			if err != nil {
				return cr.n, err
			}
			dd, err := binary.ReadVarint(cr)
			if err != nil {
				return cr.n, err
			}
			en := Entry{Leo: prev.Leo + dl, First: prev.Last + df}
			en.Last = en.First + dd
			entries = append(entries, en)
			prev = en
		}
		loaded[Key{Topic: string(topic), Partition: int32(part)}] = &series{entries: entries}
	}
	s.series = loaded
	return cr.n, nil
}

// Capacités pré-allouées au plus : les compteurs viennent du fichier, qu'une
// corruption peut rendre démesurés (allocation géante ou panique au lieu
// d'une erreur de lecture).
const (
	maxSeriesHint  = 1 << 16
	maxEntriesHint = 1 << 12
)

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

type countReader struct {
	r *bufio.Reader
	n int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countReader) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	if err == nil {
		c.n++
	}
	return b, err
}
