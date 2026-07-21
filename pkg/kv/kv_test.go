package kv

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func forEachStorage(t *testing.T, fn func(e Storage, t *testing.T)) {
	storages := map[Driver]map[string]any{
		DriverBolt:   {"path": t.TempDir()},
		DriverLegacy: {"path": filepath.Join(t.TempDir(), "test.db")},
		DriverFile:   {"path": filepath.Join(t.TempDir(), "test.json")},
	}

	for driver, opts := range storages {
		storage, err := New(driver, opts)
		require.NoError(t, err)

		t.Run(driver.String(), func(t *testing.T) {
			fn(storage, t)
		})
		assert.NoError(t, storage.Close())
	}
}

func TestNew(t *testing.T) {
	tests := map[Driver][]struct {
		name    string
		opts    map[string]any
		wantErr bool
	}{
		DriverBolt: {
			{name: "valid", opts: map[string]any{"path": t.TempDir()}, wantErr: false},
			{name: "invalid", opts: map[string]any{"path": ""}, wantErr: true},
		},
		DriverLegacy: {
			{name: "valid", opts: map[string]any{"path": filepath.Join(t.TempDir(), "test.db")}, wantErr: false},
			{name: "invalid", opts: map[string]any{"path": ""}, wantErr: true},
		},
		DriverFile: {
			{name: "valid", opts: map[string]any{"path": filepath.Join(t.TempDir(), "test.json")}, wantErr: false},
		},
		Driver("unknown"): {
			{name: "unknown", opts: map[string]any{"path": ""}, wantErr: true},
		},
	}

	for driver, tests := range tests {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%v/%s", driver, tt.name), func(t *testing.T) {
				kv, err := New(driver, tt.opts)
				if tt.wantErr {
					assert.Error(t, err)
					assert.Nil(t, kv)
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, kv)
					assert.NoError(t, kv.Close())
				}
			})
		}
	}
}

func TestStorage_Open(t *testing.T) {
	forEachStorage(t, func(e Storage, t *testing.T) {
		for _, ns := range []string{"foo", "bar", "foo"} {
			kv, err := e.Open(ns)
			require.NoError(t, err)
			require.NotNil(t, kv)
		}
	})
}

func TestStorage_Namespaces(t *testing.T) {
	namespaces := []string{"foo", "bar", "baz"}

	forEachStorage(t, func(e Storage, t *testing.T) {
		for _, ns := range namespaces {
			kv, err := e.Open(ns)
			require.NoError(t, err)
			require.NotNil(t, kv)
		}

		ns, err := e.Namespaces()
		require.NoError(t, err)
		require.ElementsMatch(t, namespaces, ns)
	})
}

func TestStorage_MigrateTo(t *testing.T) {
	meta := Meta{
		"foo": {
			"1": []byte("2"),
			"3": []byte("4"),
			"5": []byte("6"),
		},
		"bar": {
			"7":  []byte("8"),
			"9":  []byte("10"),
			"11": []byte("12"),
		},
	}

	forEachStorage(t, func(e Storage, t *testing.T) {
		for ns, pairs := range meta {
			kv, err := e.Open(ns)
			require.NoError(t, err)
			require.NotNil(t, kv)

			for key, value := range pairs {
				require.NoError(t, kv.Set(context.TODO(), key, value))
			}
		}

		m, err := e.MigrateTo()
		assert.NoError(t, err)
		assert.Equal(t, meta, m)
	})
}

// TestBolt_ConcurrentProcesses verifies that two independent bolt instances on
// the same path (simulating two tdl processes) can both open and read/write the
// same namespace. Before opening the database per operation, the second
// instance would fail with a lock timeout because the first held the file lock
// for its whole lifetime.
func TestBolt_ConcurrentProcesses(t *testing.T) {
	path := t.TempDir()
	opts := map[string]any{"path": path}

	p1, err := New(DriverBolt, opts)
	require.NoError(t, err)
	defer func() { assert.NoError(t, p1.Close()) }()

	p2, err := New(DriverBolt, opts)
	require.NoError(t, err)
	defer func() { assert.NoError(t, p2.Close()) }()

	kv1, err := p1.Open("default")
	require.NoError(t, err)
	kv2, err := p2.Open("default")
	require.NoError(t, err)

	require.NoError(t, kv1.Set(context.TODO(), "from1", []byte("a")))
	require.NoError(t, kv2.Set(context.TODO(), "from2", []byte("b")))

	// each instance sees the other's write
	v, err := kv2.Get(context.TODO(), "from1")
	require.NoError(t, err)
	require.Equal(t, []byte("a"), v)

	v, err = kv1.Get(context.TODO(), "from2")
	require.NoError(t, err)
	require.Equal(t, []byte("b"), v)
}

// TestBolt_ConcurrentReadWrite hammers a single namespace from many goroutines
// across two instances to shake out lock contention and data races.
func TestBolt_ConcurrentReadWrite(t *testing.T) {
	path := t.TempDir()
	opts := map[string]any{"path": path}

	newKV := func() Storage {
		s, err := New(DriverBolt, opts)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, s.Close()) })
		return s
	}

	kvA, err := newKV().Open("default")
	require.NoError(t, err)
	kvB, err := newKV().Open("default")
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			assert.NoError(t, kvA.Set(context.TODO(), fmt.Sprintf("a%d", i), []byte("x")))
		}(i)
		go func(i int) {
			defer wg.Done()
			assert.NoError(t, kvB.Set(context.TODO(), fmt.Sprintf("b%d", i), []byte("y")))
		}(i)
	}
	wg.Wait()

	for i := 0; i < 20; i++ {
		v, err := kvA.Get(context.TODO(), fmt.Sprintf("b%d", i))
		require.NoError(t, err)
		require.Equal(t, []byte("y"), v)
	}
}

func TestStorage_MigrateFrom(t *testing.T) {
	meta := Meta{
		"foo": {
			"1": []byte("2"),
			"3": []byte("4"),
			"5": []byte("6"),
		},
		"bar": {
			"7":  []byte("8"),
			"9":  []byte("10"),
			"11": []byte("12"),
		},
	}

	forEachStorage(t, func(e Storage, t *testing.T) {
		require.NoError(t, e.MigrateFrom(meta))

		for ns, pairs := range meta {
			kv, err := e.Open(ns)
			require.NoError(t, err)
			require.NotNil(t, kv)

			for key, value := range pairs {
				v, err := kv.Get(context.TODO(), key)
				require.NoError(t, err)
				require.Equal(t, value, v)
			}
		}
	})
}
