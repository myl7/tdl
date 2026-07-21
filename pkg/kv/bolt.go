package kv

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/mitchellh/mapstructure"
	"go.etcd.io/bbolt"

	"github.com/iyear/tdl/core/storage"
	"github.com/iyear/tdl/pkg/validator"
)

const (
	// boltOpenTimeout is how long a single bbolt.Open waits for the file lock
	// held by another process before giving up on one attempt.
	boltOpenTimeout = 2 * time.Second
	// boltOpenRetries is how many times we retry bbolt.Open on lock timeout
	// before returning a friendly error.
	boltOpenRetries = 3
)

func init() {
	register(DriverBolt, func(m map[string]any) (Storage, error) { return newBolt(m) })
}

// bolt opens the underlying database file per operation instead of holding it
// open for the whole process lifetime. bbolt takes a process-level file lock on
// Open (exclusive for read-write, shared for read-only) and only releases it on
// Close, so keeping a database open blocks every other tdl process on the same
// namespace. Opening and closing per operation shrinks that lock to the few
// milliseconds an operation actually needs, which lets multiple tdl processes
// share a namespace: the long download phase does not touch the KV at all, and
// the brief startup/shutdown writes just take turns.
//
// mu serializes access within this process so concurrent goroutines do not
// contend on the file lock (which would waste the open timeout); cross-process
// coordination is left to bbolt's file lock plus the retry in openDB.
type bolt struct {
	path string
	mu   sync.RWMutex
}

func newBolt(opts map[string]any) (*bolt, error) {
	type options struct {
		Path string `validate:"required" mapstructure:"path"`
	}

	var o options
	if err := mapstructure.WeakDecode(opts, &o); err != nil {
		return nil, errors.Wrap(err, "decode options")
	}

	if err := validator.Struct(&o); err != nil {
		return nil, errors.Wrap(err, "validate options")
	}

	if err := os.MkdirAll(o.Path, 0o755); err != nil {
		return nil, errors.Wrap(err, "create dir")
	}

	return &bolt{path: o.Path}, nil
}

func (b *bolt) Name() string {
	return DriverBolt.String()
}

// openDB opens the database file for ns. The caller MUST Close the returned db.
// readOnly opens with a shared lock so multiple processes can read at the same
// time; read-write opens with an exclusive lock. On lock contention it retries a
// few times before returning a clear error instead of a raw timeout.
func (b *bolt) openDB(ns string, readOnly bool) (*bbolt.DB, error) {
	opts := &bbolt.Options{
		Timeout:      boltOpenTimeout,
		NoGrowSync:   false,
		FreelistType: bbolt.FreelistArrayType,
		ReadOnly:     readOnly,
	}

	path := filepath.Join(b.path, ns)

	var err error
	for attempt := 0; attempt < boltOpenRetries; attempt++ {
		var db *bbolt.DB
		if db, err = bbolt.Open(path, os.ModePerm, opts); err == nil {
			return db, nil
		}
		// only a lock timeout is worth retrying; anything else is fatal
		if !errors.Is(err, bbolt.ErrTimeout) {
			return nil, errors.Wrap(err, "open db")
		}
	}

	return nil, errors.Errorf("database is busy: another tdl process is using namespace %q. "+
		"avoid running multiple tdl commands on the same namespace at the same time, then retry", ns)
}

// ensure makes sure the namespace file and its bucket exist so later read-only
// opens succeed.
func (b *bolt) ensure(ns string) error {
	path := filepath.Join(b.path, ns)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return errors.Wrap(err, "stat db")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// re-check under the lock: another goroutine may have created it meanwhile
	if _, err := os.Stat(path); err == nil {
		return nil
	}

	db, err := b.openDB(ns, false)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	return db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(ns))
		return err
	})
}

func (b *bolt) MigrateTo() (Meta, error) {
	meta := make(Meta)

	if err := b.walk(func(path string) error {
		ns := filepath.Base(path)

		b.mu.RLock()
		defer b.mu.RUnlock()

		db, err := b.openDB(ns, true)
		if err != nil {
			return errors.Wrap(err, "open")
		}
		defer func() { _ = db.Close() }()

		pairs := make(map[string][]byte)
		if err := db.View(func(tx *bbolt.Tx) error {
			bk := tx.Bucket([]byte(ns))
			if bk == nil {
				return nil
			}
			return bk.ForEach(func(k, v []byte) error {
				// copy: the mmap-backed slice is invalid after Close
				pairs[string(k)] = append([]byte(nil), v...)
				return nil
			})
		}); err != nil {
			return err
		}

		meta[ns] = pairs
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "walk")
	}

	return meta, nil
}

func (b *bolt) MigrateFrom(meta Meta) error {
	for ns, pairs := range meta {
		if err := b.writeBucket(ns, pairs); err != nil {
			return errors.Wrap(err, "update")
		}
	}

	return nil
}

func (b *bolt) writeBucket(ns string, pairs map[string][]byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	db, err := b.openDB(ns, false)
	if err != nil {
		return errors.Wrap(err, "open")
	}
	defer func() { _ = db.Close() }()

	return db.Update(func(tx *bbolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte(ns))
		if err != nil {
			return errors.Wrap(err, "create bucket")
		}
		for key, value := range pairs {
			if err = bk.Put([]byte(key), value); err != nil {
				return errors.Wrap(err, "put")
			}
		}
		return nil
	})
}

func (b *bolt) Namespaces() ([]string, error) {
	namespaces := make([]string, 0)
	if err := b.walk(func(path string) error {
		namespaces = append(namespaces, filepath.Base(path))
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "walk")
	}

	return namespaces, nil
}

func (b *bolt) walk(fn func(path string) error) error {
	return filepath.Walk(b.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.Wrap(err, "walk")
		}
		if info.IsDir() {
			return nil
		}

		return fn(path)
	})
}

func (b *bolt) Open(ns string) (storage.Storage, error) {
	if ns == "" {
		return nil, errors.New("namespace is required")
	}

	if err := b.ensure(ns); err != nil {
		return nil, errors.Wrap(err, "ensure namespace")
	}

	return &boltKV{b: b, ns: ns}, nil
}

func (b *bolt) Close() error {
	// databases are opened and closed per operation, nothing to release here
	return nil
}

type boltKV struct {
	b  *bolt
	ns string
}

func (k *boltKV) Get(_ context.Context, key string) ([]byte, error) {
	k.b.mu.RLock()
	defer k.b.mu.RUnlock()

	// no file means the namespace has never been written to
	if _, err := os.Stat(filepath.Join(k.b.path, k.ns)); os.IsNotExist(err) {
		return nil, storage.ErrNotFound
	}

	db, err := k.b.openDB(k.ns, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	var val []byte
	if err := db.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte(k.ns))
		if bk == nil {
			return nil
		}
		if v := bk.Get([]byte(key)); v != nil {
			// copy: the mmap-backed slice is invalid after Close
			val = append([]byte(nil), v...)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if val == nil {
		return nil, storage.ErrNotFound
	}
	return val, nil
}

func (k *boltKV) Set(_ context.Context, key string, value []byte) error {
	k.b.mu.Lock()
	defer k.b.mu.Unlock()

	db, err := k.b.openDB(k.ns, false)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	return db.Update(func(tx *bbolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte(k.ns))
		if err != nil {
			return errors.Wrap(err, "create bucket")
		}
		return bk.Put([]byte(key), value)
	})
}

func (k *boltKV) Delete(_ context.Context, key string) error {
	k.b.mu.Lock()
	defer k.b.mu.Unlock()

	if _, err := os.Stat(filepath.Join(k.b.path, k.ns)); os.IsNotExist(err) {
		return nil
	}

	db, err := k.b.openDB(k.ns, false)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	return db.Update(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte(k.ns))
		if bk == nil {
			return nil
		}
		return bk.Delete([]byte(key))
	})
}
