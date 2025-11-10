package store

import (
	"encoding/binary"
	"encoding/json"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)

var (
	// prefixLogs is a prefix for Raft log entries.
	prefixLogs = []byte("logs")
	// prefixConf is a prefix for Raft stable store entries (configs).
	prefixConf = []byte("conf")
)

// BadgerStore implements both raft.LogStore and raft.StableStore.
// It uses a single BadgerDB instance to store all Raft data.
type BadgerStore struct {
	db *badger.DB
}

// NewBadgerStore creates a new BadgerStore.
func NewBadgerStore(db *badger.DB) *BadgerStore {
	return &BadgerStore{db: db}
}

// uint64ToBytes converts a uint64 to a byte slice.
func uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

// bytesToUint64 converts a byte slice to a uint64.
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// --- LogStore implementation ---

// FirstIndex returns the first index written. 0 for no entries.
func (bs *BadgerStore) FirstIndex() (uint64, error) {
	var firstIndex uint64
	err := bs.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		it.Seek(prefixLogs)
		if !it.ValidForPrefix(prefixLogs) {
			firstIndex = 0
			return nil
		}
		key := it.Item().Key()
		firstIndex = bytesToUint64(key[len(prefixLogs):])
		return nil
	})
	return firstIndex, err
}

// LastIndex returns the last index written. 0 for no entries.
func (bs *BadgerStore) LastIndex() (uint64, error) {
	var lastIndex uint64
	err := bs.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Reverse:        true,
		})
		defer it.Close()

		it.Seek(append(prefixLogs, 0xff))
		if !it.ValidForPrefix(prefixLogs) {
			lastIndex = 0
			return nil
		}
		key := it.Item().Key()
		lastIndex = bytesToUint64(key[len(prefixLogs):])
		return nil
	})
	return lastIndex, err
}

// GetLog gets a log entry at a given index.
func (bs *BadgerStore) GetLog(index uint64, log *raft.Log) error {
	key := append(prefixLogs, uint64ToBytes(index)...)
	return bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return raft.ErrLogNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, log)
		})
	})
}

// StoreLog stores a single log entry.
func (bs *BadgerStore) StoreLog(log *raft.Log) error {
	return bs.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (bs *BadgerStore) StoreLogs(logs []*raft.Log) error {
	wb := bs.db.NewWriteBatch()
	defer wb.Cancel()

	for _, log := range logs {
		key := append(prefixLogs, uint64ToBytes(log.Index)...)
		val, err := json.Marshal(log)
		if err != nil {
			return err
		}
		if err := wb.Set(key, val); err != nil {
			return err
		}
	}

	return wb.Flush()
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (bs *BadgerStore) DeleteRange(min, max uint64) error {
	return bs.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		minKey := append(prefixLogs, uint64ToBytes(min)...)
		for it.Seek(minKey); it.ValidForPrefix(prefixLogs); it.Next() {
			key := it.Item().KeyCopy(nil)
			index := bytesToUint64(key[len(prefixLogs):])
			if index > max {
				break
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// --- StableStore implementation ---

// Set is used to set a key/value set.
func (bs *BadgerStore) Set(key []byte, val []byte) error {
	return bs.db.Update(func(txn *badger.Txn) error {
		return txn.Set(append(prefixConf, key...), val)
	})
}

// Get is used to retrieve a value for a given key.
func (bs *BadgerStore) Get(key []byte) ([]byte, error) {
	var val []byte
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(append(prefixConf, key...))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil // Key not found is not an error for Raft
			}
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

// SetUint64 is used to set a key/value set of the uint64 type.
func (bs *BadgerStore) SetUint64(key []byte, val uint64) error {
	return bs.Set(key, uint64ToBytes(val))
}

// GetUint64 is used to retrieve a value for a given key of the uint64 type.
func (bs *BadgerStore) GetUint64(key []byte) (uint64, error) {
	val, err := bs.Get(key)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}
	return bytesToUint64(val), nil
}