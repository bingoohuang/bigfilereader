package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/multierr"
)

func main() {
	pPort := flag.Int("port", 8080, "listen port")
	flag.Parse()

	db := &pebbleDB{}
	if err := db.Open("labelsdb/db", Partitions); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	r := httprouter.New()
	r.POST("/load/:file/:label", wrapHandler(db.LoadFile))
	r.GET("/labels/:mobile", wrapHandler(db.GetLabel))

	log.Printf("Listening on %d", *pPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *pPort), r))
}

func wrapHandler(h func(http.ResponseWriter, *http.Request, httprouter.Params) error) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if err := h(w, r, p); err != nil {
			jsonResponseError(w, err)
		}
	}
}

// H is alias for map[string]any.
type H map[string]any

func jsonResponse(w http.ResponseWriter, body H) error {
	if err := json.NewEncoder(w).Encode(H{"body": body, "status": "ok"}); err != nil {
		log.Printf("encode json response failed: %v", err)
	}
	return nil
}

func jsonResponseError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)

	if err := json.NewEncoder(w).Encode(H{"status": "error", "error": err.Error()}); err != nil {
		log.Printf("encode json response failed: %v", err)
	}
}

func scanFilePart(file string, wg *sync.WaitGroup, lineCallback func(line string) error, start, end int, chop *Chop) error {
	defer wg.Done()

	f, err := os.OpenFile(file, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if start > 0 {
		if _, err := f.Seek(int64(start), io.SeekStart); err != nil {
			log.Fatal(err)
		}
	}

	var line []byte
	countBytes := end - start
	const bufferSize = 16 * 1024
	buffer := make([]byte, bufferSize)
	lines := 0
	lineStarted := false
	for total := 0; total < countBytes; {
		n, err := f.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		total += n
		if total > countBytes {
			n -= total - countBytes
		}

		bb := buffer[:n]
		for _, b := range bb {
			if IsSpace(b) {
				if b == '\n' {
					chop.linebreak = true
					if !lineStarted {
						lineStarted = true
					}

					if len(line) > 0 {
						lines++
						if err := lineCallback(strings.TrimSpace(string(line))); err != nil {
							return err
						}
						line = line[:0]
					}
				}
			} else if lineStarted {
				line = append(line, b)
			} else {
				chop.head = append(chop.head, b)
			}
		}
	}
	chop.tail = append(chop.tail, line...)
	return nil
}

func IsSpace(b byte) bool {
	switch b {
	case ' ', '\t', '\r', '\v', '\f', '\n':
		return true
	default:
		return false
	}
}

type Chop struct {
	head      []byte
	tail      []byte
	linebreak bool
}

func scanFile(file string, syncMode bool, lineCallback func(line string) error) error {
	stat, err := os.Stat(file)
	if err != nil {
		return err
	}

	numWorkers := runtime.NumCPU()
	fileSize := int(stat.Size())
	workerSize := fileSize / numWorkers
	var wg sync.WaitGroup

	chops := make([]*Chop, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * workerSize
		end := start + workerSize
		if end > fileSize {
			end = fileSize
		}

		chops[i] = &Chop{}
		wg.Add(1)
		if !syncMode {
			go func(c *Chop, start, end int) {
				if err := scanFilePart(file, &wg, lineCallback, start, end, c); err != nil {
					log.Fatal(err)
				}
			}(chops[i], start, end)
		} else if err := scanFilePart(file, &wg, lineCallback, start, end, chops[i]); err != nil {
			return err
		}
	}

	wg.Wait()

	var line []byte

	for i := 0; i < numWorkers; i++ {
		chop := chops[i]
		line = append(line, chop.head...)
		if chop.linebreak {
			if len(line) > 0 {
				if err := lineCallback(string(line)); err != nil {
					return err
				}
				line = line[:0]
			}
		}
		line = append(line, chop.tail...)
	}
	if len(line) > 0 {
		if err := lineCallback(string(line)); err != nil {
			return err
		}
	}

	return nil
}

func Hash(data []byte) uint64 {
	h := xxhash.New()
	h.Write(data)
	return h.Sum64()
}

type pebbleDB struct {
	dbs []*pebble.DB // Primary data
	dbc []chan op
	sync.WaitGroup
}

func (s *pebbleDB) GetLabel(w http.ResponseWriter, r *http.Request, p httprouter.Params) error {
	start := time.Now()
	mobile, err := mobile2bytes(p.ByName("mobile"))
	if err != nil {
		return err
	}

	labels, err := s.FindLabelsByMobile(mobile)
	if err != nil {
		return err
	}

	cost := time.Since(start)
	return jsonResponse(w, H{"cost": cost.String(), "labels": labels})
}

func IsBool(s string) bool {
	return FoldAnyOf(s, "y", "1", "t", "yes", "true", "on")
}

func FoldAnyOf(t string, bb ...string) bool {
	for _, b := range bb {
		if strings.EqualFold(t, b) {
			return true
		}
	}

	return false
}

func (s *pebbleDB) LoadFile(w http.ResponseWriter, r *http.Request, p httprouter.Params) error {
	file := p.ByName("file")
	label := p.ByName("label")
	noop := IsBool(r.URL.Query().Get("noop"))
	syncMode := IsBool(r.URL.Query().Get("sync"))
	log.Printf("start to load file %s", file)
	start := time.Now()
	var lines atomic.Uint64
	if err := scanFile(file, syncMode, func(line string) error {
		lines.Add(1)
		if !noop {
			mobile, err := mobile2bytes(line)
			if err != nil {
				return err
			}
			s.Append(mobile, []byte(label))
			return nil
		}
		return nil
	}); err != nil {
		return err
	}
	cost := time.Since(start)
	log.Printf("load file: %s with label: %s, lines: %d, sync: %t complete, cost %s", file, label, lines.Load(), syncMode, cost)
	return jsonResponse(w, H{"cost": cost.String(), "lines": lines.Load()})
}

func (s *pebbleDB) FindLabelsByMobile(mobile []byte) (labels []string, err error) {
	partition := s.Partition(mobile)
	db := s.dbs[partition]

	keyUpperBound := func(b []byte) []byte {
		end := make([]byte, len(b))
		copy(end, b)
		for i := len(end) - 1; i >= 0; i-- {
			end[i] += 1
			if end[i] != 0 {
				return end[:i+1]
			}
		}
		return nil // no upper-bound
	}

	prefixIterOptions := func(prefix []byte) *pebble.IterOptions {
		return &pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: keyUpperBound(prefix),
		}
	}
	iter := db.NewIter(prefixIterOptions(mobile))
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		labels = append(labels, string(key[len(mobile):]))
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	return labels, err
}

func (s *pebbleDB) Append(key, value []byte) {
	partition := s.Partition(key)
	s.dbc[partition] <- op{
		typ:   opSet,
		key:   append(key, value...),
		value: []byte{},
	}
}

func (s *pebbleDB) Get(key []byte) (values []string, err error) {
	partition := s.Partition(key)
	value, closer, err := s.dbs[partition].Get(key)
	if closer != nil {
		defer closer.Close()
	}
	if err != nil {
		return nil, err
	}

	if err == pebble.ErrNotFound {
		return nil, nil
	}

	for _, l := range bytes.Split(value, []byte(",")) {
		values = append(values, string(l))
	}

	return values, nil
}

// Set implements DB
func (s *pebbleDB) Set(key, value []byte) {
	partition := s.Partition(key)
	s.dbc[partition] <- op{
		typ:   opSet,
		key:   key,
		value: value,
	}
}

// Close implements DB
func (s *pebbleDB) Close() (err error) {
	for _, db := range s.dbc {
		close(db)
	}
	s.Wait()

	for _, db := range s.dbs {
		err = multierr.Append(err, db.Close())
	}
	return err
}

type opType uint32

const (
	_ opType = iota
	opSet
	opAppend
)

type op struct {
	typ        opType
	key, value []byte
}

// Open implements DB
func (s *pebbleDB) Open(path string, partitions uint64) (err error) {
	s.dbs = make([]*pebble.DB, partitions)
	s.dbc = make([]chan op, partitions)
	for i := uint64(0); i < partitions; i++ {
		name := fmt.Sprintf("%s.%d", path, i)
		s.dbs[i], err = pebble.Open(name, &pebble.Options{})
		if err != nil {
			return err
		}

		s.dbc[i] = make(chan op, 10000)
		s.Add(1)
		go func(db *pebble.DB, c chan op) {
			defer s.Done()

			for k := range c {
				switch k.typ {
				case opSet:
					if err := db.Set(k.key, k.value, pebble.NoSync); err != nil {
						log.Fatal(err)
					}
				case opAppend:
					v, closer, err := db.Get(k.key)
					if err == pebble.ErrNotFound {
						err = nil
					}
					if err != nil {
						log.Fatal(err)
					}
					if len(v) > 0 {
						k.value = append(k.value, ',')
						k.value = append(k.value, v...)
					}
					if closer != nil {
						closer.Close()
					}

					if err := db.Set(k.key, k.value, pebble.NoSync); err != nil {
						log.Fatal(err)
					}
				}
			}
		}(s.dbs[i], s.dbc[i])
	}

	return nil
}

func (s *pebbleDB) Partition(partitionKey []byte) uint64 {
	return Hash(partitionKey) % Partitions
}

var Partitions = uint64(10)

func init() {
	if p := os.Getenv("PARTITIONS"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			Partitions = uint64(n)
		}
	}
}

func mobile2bytes(s string) ([]byte, error) {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return nil, err
	}

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, u)
	return b, nil
}

func bytes2uint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}
