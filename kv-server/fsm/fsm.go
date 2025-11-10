package fsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)

// command represents a command to be applied to the FSM.
type command struct {
	Op    string `json:"op,omitempty"`
	Key   []byte `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
}

// BadgerFSM implements the raft.FSM interface and is the core state machine
// for our key-value store. It applies Raft logs to the BadgerDB database.
type BadgerFSM struct {
	db *badger.DB
}

// NewBadgerFSM creates a new BadgerFSM.
func NewBadgerFSM(db *badger.DB) *BadgerFSM {
	return &BadgerFSM{db: db}
}

// Apply applies a Raft log entry to the key-value store.
func (f *BadgerFSM) Apply(log *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(log.Data, &c); err != nil {
		panic("failed to unmarshal command: " + err.Error())
	}

	db := f.db
	switch c.Op {
	case "SET":
		return db.Update(func(txn *badger.Txn) error {
			return txn.Set(c.Key, c.Value)
		})
	case "DELETE":
		return db.Update(func(txn *badger.Txn) error {
			return txn.Delete(c.Key)
		})
	default:
		panic("unrecognized command op: " + c.Op)
	}
}

// fsmSnapshot implements the raft.FSMSnapshot interface.
type fsmSnapshot struct {
	db *badger.DB
}

// Persist writes the FSM state to a sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			if bytes.HasPrefix(key, []byte("logs")) || bytes.HasPrefix(key, []byte("conf")) {
				continue
			}

			err := item.Value(func(val []byte) error {
				keyLen := uint64(len(key))
				if err := binary.Write(sink, binary.BigEndian, keyLen); err != nil {
					return err
				}
				if _, err := sink.Write(key); err != nil {
					return err
				}
				valLen := uint64(len(val))
				if err := binary.Write(sink, binary.BigEndian, valLen); err != nil {
					return err
				}
				if _, err := sink.Write(val); err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

// Release is a no-op.
func (s *fsmSnapshot) Release() {}

// Snapshot returns a snapshot of the current state.
func (f *BadgerFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{db: f.db}, nil
}

// Restore is used to restore the FSM from a snapshot.
func (f *BadgerFSM) Restore(rc io.ReadCloser) error {
	wb := f.db.NewWriteBatch()
	defer wb.Cancel()

	reader := bufio.NewReader(rc)
	for {
		var keyLen, valLen uint64

		err := binary.Read(reader, binary.BigEndian, &keyLen)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return err
		}

		if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
			return err
		}

		val := make([]byte, valLen)
		if _, err := io.ReadFull(reader, val); err != nil {
			return err
		}

		if err := wb.Set(key, val); err != nil {
			return err
		}
	}

	return wb.Flush()
}
