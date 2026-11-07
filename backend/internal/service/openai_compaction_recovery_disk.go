package service

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	openAICompactionRecoveryDurableBytes = 64 << 20
	openAICompactionRecoveryDiskBytes    = 128 << 20
	openAICompactionRecoveryDiskRecord   = 24 << 20
)

// Only a successful compaction's bounded undo/replacement is stored. Ordinary
// requests and response deltas never become disk records. The worker performs
// encryption and IO off the forwarding path; shutdown drains pending updates.
type openAICompactionRecoveryDisk struct {
	root    *os.Root
	aead    cipher.AEAD
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
	written map[string]compactionDiskStamp
	warned  time.Time
}

type compactionDiskStamp struct {
	expires        time.Time
	invalid        bool
	limitAccount   int64
	limitCondition [32]byte
}

type compactionDiskRecord struct {
	Version         int       `json:"version"`
	Scope           string    `json:"scope"`
	Owner           string    `json:"owner,omitempty"`
	Key             string    `json:"key"`
	Input           []byte    `json:"input"` // already gzip compressed
	Fingerprints    []byte    `json:"fingerprints"`
	CompactionIndex int       `json:"compaction_index"`
	Invalid         bool      `json:"invalid"`
	Replacement     bool      `json:"replacement"`
	AccountID       int64     `json:"account_id"`
	Condition       [32]byte  `json:"condition"`
	Expires         time.Time `json:"expires"`
	LimitAccount    int64     `json:"limit_account,omitempty"`
	LimitCondition  [32]byte  `json:"limit_condition,omitempty"`
}

func compactionDiskName(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:]) + ".snapshot"
}

func compactionDiskOwned(name string) bool {
	name = strings.TrimSuffix(name, ".tmp")
	if len(name) != 64+len(".snapshot") || !strings.HasSuffix(name, ".snapshot") {
		return false
	}
	_, err := hex.DecodeString(name[:64])
	return err == nil && name == strings.ToLower(name)
}

// StartCompactionRecoveryPersistence is called only by the production server,
// before accepting requests. Embedded callers and tests remain memory-only.
func (s *OpenAIGatewayService) StartCompactionRecoveryPersistence() {
	if s.cfg == nil || strings.TrimSpace(s.cfg.JWT.Secret) == "" {
		return
	}
	base := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if base == "" {
		base = "./data"
	}
	if err := s.compactionRecoveryStore().startPersistence(filepath.Join(base, "compaction-recovery"), s.cfg.JWT.Secret); err != nil {
		// Never print paths, secrets or recovered contents in an IO error.
		logOpenAIWSModeWarn("compaction_recovery_persistence_unavailable phase=start")
		return
	}
	store := s.compactionRecoveryStore()
	store.mu.Lock()
	loaded := len(store.entries)
	store.mu.Unlock()
	logOpenAIWSModeInfo("compaction_recovery_persistence_ready snapshots=%d retention_seconds=%d disk_limit_bytes=%d", loaded, int(openAICompactionRecoveryTTL.Seconds()), openAICompactionRecoveryDiskBytes)
}

func (s *openAICompactionRecoveryStore) startPersistence(dir, secret string) error {
	if secret == "" || s.disk != nil || len(s.entries) != 0 {
		return errors.New("compaction recovery persistence requires an empty store and a stable secret")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	key := sha256.Sum256([]byte("sub2api/compaction-recovery/v1\x00" + secret))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		root.Close()
		return err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		root.Close()
		return err
	}
	d := &openAICompactionRecoveryDisk{root: root, aead: aead, wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}), written: make(map[string]compactionDiskStamp)}
	s.disk = d
	if err := d.load(s); err != nil {
		// Keep any verified in-memory entries, but do not pretend persistence
		// is available. Their normal expiry timers still bound retention.
		s.mu.Lock()
		s.disk = nil
		s.mu.Unlock()
		root.Close()
		return err
	}
	go func() {
		defer close(d.done)
		defer d.root.Close()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			closing := false
			select {
			case <-d.stop:
				closing = true
			case <-d.wake:
			case <-ticker.C:
			}
			if err := d.flush(s, closing); err != nil && time.Since(d.warned) >= time.Minute {
				d.warned = time.Now()
				logOpenAIWSModeWarn("compaction_recovery_persistence_unavailable phase=flush")
			}
			if closing {
				return
			}
		}
	}()
	return nil
}

func (s *openAICompactionRecoveryStore) notifyPersistence() {
	if s.disk != nil {
		select {
		case s.disk.wake <- struct{}{}:
		default:
		}
	}
}

func (s *openAICompactionRecoveryStore) closePersistence() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	for _, e := range s.entries {
		e.timer.Stop()
	}
	d := s.disk
	s.mu.Unlock()
	if d != nil {
		d.once.Do(func() { close(d.stop) })
		<-d.done
	}
}

func (d *openAICompactionRecoveryDisk) files() ([]os.FileInfo, int64, error) {
	f, err := d.root.Open(".")
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	entries, err := f.ReadDir(-1)
	if err != nil {
		return nil, 0, err
	}
	var files []os.FileInfo
	var total int64
	for _, e := range entries {
		if !e.Type().IsRegular() || !compactionDiskOwned(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, 0, err
		}
		files = append(files, info)
		total += info.Size()
	}
	sort.Slice(files, func(i, j int) bool { return files[i].ModTime().Before(files[j].ModTime()) })
	return files, total, nil
}

func (d *openAICompactionRecoveryDisk) load(s *openAICompactionRecoveryStore) error {
	files, total, err := d.files()
	if err != nil {
		return err
	}
	for _, f := range files {
		name := f.Name()
		if strings.HasSuffix(name, ".tmp") || f.Size() > openAICompactionRecoveryDiskRecord || total > openAICompactionRecoveryDiskBytes {
			if err := d.root.Remove(name); err != nil {
				return err
			}
			total -= f.Size()
			continue
		}
		file, err := d.root.Open(name)
		if err != nil {
			return err
		}
		sealed, readErr := io.ReadAll(io.LimitReader(file, openAICompactionRecoveryDiskRecord+1))
		file.Close()
		var r compactionDiskRecord
		valid := readErr == nil && len(sealed) <= openAICompactionRecoveryDiskRecord && len(sealed) >= d.aead.NonceSize()
		if valid {
			raw, err := d.aead.Open(nil, sealed[:d.aead.NonceSize()], sealed[d.aead.NonceSize():], []byte(name))
			valid = err == nil && json.Unmarshal(raw, &r) == nil
		}
		limit := openAIWSContinuationOutputLimit
		if r.Replacement {
			limit = openAIWSContinuationHistoryLimit
		}
		valid = valid && (r.Version == 1 || r.Version == 2) && (r.Version != 1 || r.Owner == "") && r.Scope != "" && strings.HasPrefix(r.Key, r.Scope+":") && compactionDiskName(r.Key) == name &&
			len(r.Input) > 0 && len(r.Input) <= openAICompactionRecoveryMaxEntry && len(r.Fingerprints) > 0 && len(r.Fingerprints)%32 == 0 && len(r.Fingerprints)/32 <= limit &&
			time.Now().Before(r.Expires) && !r.Expires.After(time.Now().Add(openAICompactionRecoveryTTL)) &&
			((r.Replacement && r.AccountID > 0) || (!r.Replacement && r.CompactionIndex >= 0 && r.CompactionIndex < len(r.Fingerprints)/32))
		if valid {
			e := &openAICompactionRecoveryEntry{scope: r.Scope, owner: r.Owner, key: r.Key, input: r.Input, compactionIndex: r.CompactionIndex,
				invalid: r.Invalid, replacement: r.Replacement, accountID: r.AccountID, condition: r.Condition, expires: r.Expires,
				limitAccount: r.LimitAccount, limitCondition: r.LimitCondition}
			e.output = make([][32]byte, len(r.Fingerprints)/32)
			for i := range e.output {
				copy(e.output[i][:], r.Fingerprints[i*32:(i+1)*32])
			}
			valid = s.insert(e)
		}
		if !valid {
			if err := d.root.Remove(name); err != nil {
				return err
			}
			continue
		}
		d.written[name] = compactionDiskStamp{r.Expires, r.Invalid, r.LimitAccount, r.LimitCondition}
	}
	return d.flush(s, false)
}

func (d *openAICompactionRecoveryDisk) flush(s *openAICompactionRecoveryStore, force bool) error {
	s.mu.Lock()
	records := make(map[string]openAICompactionRecoveryEntry, len(s.entries))
	for _, e := range s.entries {
		if !time.Now().Before(e.expires) {
			continue
		}
		// Both byte arrays are immutable. Copy metadata under the lock; do
		// serialization/encryption and IO after releasing the forwarding lock.
		records[compactionDiskName(e.key)] = *e
	}
	s.mu.Unlock()
	files, total, err := d.files()
	if err != nil {
		return err
	}
	sizes := make(map[string]int64)
	for _, f := range files {
		if _, keep := records[f.Name()]; !keep {
			if err := d.root.Remove(f.Name()); err != nil {
				return err
			}
			total -= f.Size()
			delete(d.written, f.Name())
		} else {
			sizes[f.Name()] = f.Size()
		}
	}
	for name, e := range records {
		old, exists := d.written[name]
		// Refresh expiry at most every five minutes while a snapshot is used.
		// Rejections and new snapshots are written immediately; shutdown flushes all.
		if exists && sizes[name] > 0 && old.invalid == e.invalid && old.limitAccount == e.limitAccount && old.limitCondition == e.limitCondition && (old.expires.Equal(e.expires) || (!force && e.expires.Sub(old.expires) < 5*time.Minute)) {
			continue
		}
		fingerprints := make([]byte, 0, len(e.output)*32)
		for _, hash := range e.output {
			fingerprints = append(fingerprints, hash[:]...)
		}
		r := compactionDiskRecord{Version: 2, Scope: e.scope, Owner: e.owner, Key: e.key, Input: e.input, Fingerprints: fingerprints,
			CompactionIndex: e.compactionIndex, Invalid: e.invalid, Replacement: e.replacement, AccountID: e.accountID, Condition: e.condition, Expires: e.expires,
			LimitAccount: e.limitAccount, LimitCondition: e.limitCondition}
		raw, err := json.Marshal(r)
		if err != nil {
			return err
		}
		nonce := make([]byte, d.aead.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return err
		}
		sealed := d.aead.Seal(nonce, nonce, raw, []byte(name))
		if len(sealed) > openAICompactionRecoveryDiskRecord || total+int64(len(sealed)) > openAICompactionRecoveryDiskBytes {
			return errors.New("compaction recovery disk budget exceeded")
		}
		if err := d.write(name, sealed); err != nil {
			return err
		}
		total += int64(len(sealed)) - sizes[name]
		d.written[name] = compactionDiskStamp{r.Expires, r.Invalid, r.LimitAccount, r.LimitCondition}
	}
	return nil
}

func (d *openAICompactionRecoveryDisk) write(name string, sealed []byte) error {
	temp := name + ".tmp"
	f, err := d.root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer d.root.Remove(temp)
	_, writeErr := f.Write(sealed)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return d.root.Rename(temp, name)
}
