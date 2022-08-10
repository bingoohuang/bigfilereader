package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/multierr"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
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

	router := httprouter.New()
	router.POST("/load/:file/:label", wrapHandler(func(w http.ResponseWriter, r *http.Request, p httprouter.Params) error {
		file := p.ByName("file")
		label := p.ByName("label")
		noop := r.URL.Query().Has("noop")
		log.Printf("start to load file %s", file)
		start := time.Now()
		lines := 0
		if err := scanFile(file, func(line string) {
			lines++
			if !noop {
				db.Set([]byte(line), []byte(line+label))
			}
		}); err != nil {
			return err
		}
		cost := time.Since(start)
		log.Printf("load file %s with label %s complete, cost %s", file, label, cost)
		return jsonResponse(w, H{"cost": cost.String(), "lines": lines})
	}))

	router.GET("/labels/:mobile", wrapHandler(func(w http.ResponseWriter, r *http.Request, p httprouter.Params) error {
		mobile := p.ByName("mobile")
		mobileBytes := []byte(mobile)
		start := time.Now()
		labels := db.FindLabelsByMobile(mobileBytes, mobileBytes)
		cost := time.Since(start)
		return jsonResponse(w, H{"cost": cost.String(), "labels": labels})
	}))

	log.Printf("Listening on %d", *pPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *pPort), router))
}

func wrapHandler(h func(http.ResponseWriter, *http.Request, httprouter.Params) error) func(http.ResponseWriter, *http.Request, httprouter.Params) {
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

func scanFile(file string, lineCallback func(line string)) error {
	f, err := os.OpenFile(file, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lineCallback(sc.Text())
	}
	return sc.Err()
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
