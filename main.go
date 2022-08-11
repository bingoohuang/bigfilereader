package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/multierr"
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

func scanFilePart(file string, wg *sync.WaitGroup, lineCallback func(line string), start, end int, chop *Chop) {
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
						lineCallback(strings.TrimSpace(string(line)))
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

func scanFile(file string, lineCallback func(line string)) error {
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
		go scanFilePart(file, &wg, lineCallback, start, end, chops[i])
	}

	wg.Wait()

	var line []byte

	for i := 0; i < numWorkers; i++ {
		chop := chops[i]
		line = append(line, chop.head...)
		if chop.linebreak {
			if len(line) > 0 {
				lineCallback(string(line))
				line = line[:0]
			}
		}
		line = append(line, chop.tail...)
	}
	if len(line) > 0 {
		lineCallback(string(line))
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
	dbc []chan []byte
	sync.WaitGroup
}

func (s *pebbleDB) GetLabel(w http.ResponseWriter, r *http.Request, p httprouter.Params) error {
	mobile := p.ByName("mobile")
	mobileBytes := []byte(mobile)
	start := time.Now()
	labels := s.FindLabelsByMobile(mobileBytes, mobileBytes)
	cost := time.Since(start)
	return jsonResponse(w, H{"cost": cost.String(), "labels": labels})
}

func (s *pebbleDB) LoadFile(w http.ResponseWriter, r *http.Request, p httprouter.Params) error {
	file := p.ByName("file")
	label := p.ByName("label")
	noop := r.URL.Query().Has("noop")
	log.Printf("start to load file %s", file)
	start := time.Now()
	var lines atomic.Uint64
	if err := scanFile(file, func(line string) {
		lines.Add(1)
		if !noop {
			s.Set([]byte(line), []byte(line+label))
		}
	}); err != nil {
		return err
	}
	cost := time.Since(start)
	log.Printf("load file %s with label %s andl lines %d complete, cost %s", file, label, lines.Load(), cost)
	return jsonResponse(w, H{"cost": cost.String(), "lines": lines.Load()})
}

func (s *pebbleDB) FindLabelsByMobile(partitionKey, mobile []byte) (labels []string) {
	partition := s.Partition(partitionKey)
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
		log.Fatal(err)
	}

	return labels
}

// Set implements DB
func (s *pebbleDB) Set(partitionKey, key []byte) {
	partition := s.Partition(partitionKey)
	s.dbc[partition] <- key
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

var zeroBytes = make([]byte, 0)

// Open implements DB
func (s *pebbleDB) Open(path string, partitions uint64) (err error) {
	s.dbs = make([]*pebble.DB, partitions)
	s.dbc = make([]chan []byte, partitions)
	for i := uint64(0); i < partitions; i++ {
		name := fmt.Sprintf("%s.%d", path, i)
		s.dbs[i], err = pebble.Open(name, &pebble.Options{})
		if err != nil {
			return err
		}

		s.dbc[i] = make(chan []byte, 10000)
		s.Add(1)
		go func(db *pebble.DB, c chan []byte) {
			defer s.Done()

			for k := range c {
				if err := db.Set(k, zeroBytes, pebble.NoSync); err != nil {
					log.Fatal(err)
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
