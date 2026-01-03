package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	dir          = ".output"
	walPrefix    = "wal_"
	snapshotFile = "snapshot.json"
	segmentSize  = 5
)

/* -------------------- DATA -------------------- */

type Entry struct {
	Seq   int
	Key   string
	Delta int
}

type Snapshot struct {
	LastSeq int
	State   map[string]int
}

type WAL struct {
	state       map[string]int
	appliedSeq  int // moves during replay / append
	snapshotSeq int // immutable boundary
}

/* -------------------- INIT -------------------- */

func NewWAL() *WAL {
	os.MkdirAll(dir, 0755)
	w := &WAL{
		state: make(map[string]int),
	}
	w.loadSnapshot()
	w.replay()
	return w
}

/* -------------------- APPEND -------------------- */

func (w *WAL) Append(key string, delta int) {
	seq := w.appliedSeq + 1
	segment := seq / segmentSize

	fname := filepath.Join(dir, fmt.Sprintf("%s%03d.log", walPrefix, segment))
	f, _ := os.OpenFile(fname, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	msg := fmt.Sprintf("%d:%s:%d", seq, key, delta)
	hash := sha256.Sum256([]byte(msg))
	fmt.Fprintf(f, "%s:%x\n", msg, hash)

	f.Sync()
	f.Close()

	w.appliedSeq = seq
	w.state[key] += delta
}

/* -------------------- SNAPSHOT -------------------- */

func (w *WAL) Snapshot() {
	snap := Snapshot{
		LastSeq: w.appliedSeq,
		State:   w.state,
	}

	data, _ := json.MarshalIndent(snap, "", "  ")
	os.WriteFile(filepath.Join(dir, snapshotFile), data, 0644)

	w.snapshotSeq = snap.LastSeq
}

/* -------------------- REPLAY -------------------- */

func (w *WAL) loadSnapshot() {
	data, err := os.ReadFile(filepath.Join(dir, snapshotFile))
	if err != nil {
		return
	}

	var snap Snapshot
	json.Unmarshal(data, &snap)

	w.state = snap.State
	w.snapshotSeq = snap.LastSeq
	w.appliedSeq = snap.LastSeq
}

func (w *WAL) replay() {
	files, _ := filepath.Glob(filepath.Join(dir, walPrefix+"*.log"))

	for _, fname := range files {
		f, _ := os.Open(fname)
		scanner := bufio.NewScanner(f)

		for scanner.Scan() {
			seq, key, delta, hash := parse(scanner.Text())

			if seq <= w.snapshotSeq {
				continue
			}
			if !isValidHash(seq, key, delta, hash) {
				continue
			}

			w.state[key] += delta
			w.appliedSeq = max(w.appliedSeq, seq)
		}
		f.Close()
	}
}

/* -------------------- HELPERS -------------------- */

func parse(line string) (int, string, int, string) {
	parts := strings.Split(line, ":")
	seq, _ := strconv.Atoi(parts[0])
	delta, _ := strconv.Atoi(parts[2])
	return seq, parts[1], delta, parts[3]
}

func isValidHash(seq int, key string, delta int, expected string) bool {
	msg := fmt.Sprintf("%d:%s:%d", seq, key, delta)
	actual := fmt.Sprintf("%x", sha256.Sum256([]byte(msg)))
	return actual == expected
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

/* -------------------- DEMO -------------------- */

func main() {
	w := NewWAL()
	//
	// w.Append("a", 5)
	// w.Append("a", 3)
	// w.Append("b", 10)
	//
	// w.Snapshot()
	//
	// w.Append("a", 2)
	// w.Append("b", -4)

	fmt.Println("STATE:", w.state)
}

