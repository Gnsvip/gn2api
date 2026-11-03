package requesttiming

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MaxDiskBytes   int64 = 50 * 1024 * 1024
	MaxRecordBytes       = 16 * 1024
	Retention            = 7 * 24 * time.Hour
	segmentBytes   int64 = 1024 * 1024
)

// Journal owns only timing-<unixnano>.jsonl files in its private directory.
// The cap includes the active file. Rotation/pruning is synchronous, so unlike
// backup-count rotation it cannot temporarily exceed the cap. No DB/stdout copy.
type Journal struct {
	mu        sync.Mutex
	dir       string
	files     []segment
	limit     int64
	retention time.Duration
}
type segment struct {
	name string
	born time.Time
	size int64
}

func OpenJournal(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	j := &Journal{dir: dir, limit: MaxDiskBytes, retention: Retention}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(e.Name(), "timing-"), ".jsonl"), 10, 64)
		if err != nil || e.Name() != fmt.Sprintf("timing-%d.jsonl", n) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		j.files = append(j.files, segment{e.Name(), time.Unix(0, n), info.Size()})
	}
	sort.Slice(j.files, func(a, b int) bool { return j.files[a].born.Before(j.files[b].born) })
	if err := j.prune(time.Now(), 0); err != nil {
		return nil, err
	}
	return j, nil
}
func (j *Journal) prune(now time.Time, extra int64) error {
	var total int64
	for _, f := range j.files {
		total += f.size
	}
	for len(j.files) > 0 && (total+extra > j.limit || !j.files[0].born.After(now.Add(-j.retention))) {
		f := j.files[0]
		if err := os.Remove(filepath.Join(j.dir, f.name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		total -= f.size
		j.files = j.files[1:]
	}
	return nil
}
func (j *Journal) Prune() error { j.mu.Lock(); defer j.mu.Unlock(); return j.prune(time.Now(), 0) }
func (j *Journal) Write(s Summary) error {
	if !s.NeedsEvidence() {
		return nil
	}
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if len(b) > MaxRecordBytes {
		return errors.New("request timing record exceeded fixed bound")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	now := time.Now()
	if int64(len(b)) > j.limit {
		return errors.New("request timing record exceeded disk cap")
	}
	if err := j.prune(now, int64(len(b))); err != nil {
		return err
	}
	if len(j.files) == 0 || j.files[len(j.files)-1].size+int64(len(b)) > segmentBytes || now.Sub(j.files[len(j.files)-1].born) >= time.Hour {
		j.files = append(j.files, segment{name: fmt.Sprintf("timing-%d.jsonl", now.UnixNano()), born: now})
	}
	f := &j.files[len(j.files)-1]
	file, err := os.OpenFile(filepath.Join(j.dir, f.name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	n, writeErr := file.Write(b)
	f.size += int64(n)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
