package handler

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	kv "github.com/your-username/mytidb/kv-server/proto"
)

// command represents a command to be applied to the FSM.
type command struct {
	Op    string `json:"op,omitempty"`
	Key   []byte `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
}

// KVHandler implements the gRPC server.
type KVHandler struct {
	kv.UnimplementedKVServer
	db       *badger.DB
	logger   *zap.Logger
	raftNode *raft.Raft
}

// NewKVHandler creates a new KVHandler.
func NewKVHandler(db *badger.DB, logger *zap.Logger, raftNode *raft.Raft) *KVHandler {
	return &KVHandler{
		db:       db,
		logger:   logger,
		raftNode: raftNode,
	}
}

// Join adds a new node to the Raft cluster.
func (h *KVHandler) Join(ctx context.Context, req *kv.JoinRequest) (*kv.JoinResponse, error) {
	h.logger.Info("received join request", zap.Uint64("node_id", req.NodeId), zap.String("raft_addr", req.RaftAddr))

	if h.raftNode.State() != raft.Leader {
		leaderAddr := h.raftNode.Leader()
		h.logger.Warn("not the leader, forwarding join request", zap.String("leader_addr", string(leaderAddr)))
		return nil, status.Errorf(codes.FailedPrecondition, "not the leader, current leader is at %s", leaderAddr)
	}

	serverID := raft.ServerID(strconv.FormatUint(req.NodeId, 10))
	h.logger.Info("adding voter", zap.String("server_id", string(serverID)), zap.String("raft_addr", req.RaftAddr))

	addVoterFuture := h.raftNode.AddVoter(serverID, raft.ServerAddress(req.RaftAddr), 0, 0)
	if err := addVoterFuture.Error(); err != nil {
		h.logger.Error("failed to add voter", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to add voter: %v", err)
	}

	h.logger.Info("successfully added voter", zap.String("server_id", string(serverID)))
	return &kv.JoinResponse{}, nil
}

// Put implements kv.KVServer
func (h *KVHandler) Put(ctx context.Context, in *kv.PutRequest) (*kv.PutResponse, error) {
	h.logger.Info("Received Put request", zap.ByteString("key", in.GetKey()))

	if h.raftNode.State() != raft.Leader {
		return nil, status.Errorf(codes.FailedPrecondition, "not leader")
	}

	cmd := command{Op: "SET", Key: in.GetKey(), Value: in.GetValue()}
	cBytes, err := json.Marshal(cmd)
	if err != nil {
		h.logger.Error("Failed to marshal Put command", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to marshal command: %v", err)
	}

	future := h.raftNode.Apply(cBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		h.logger.Error("Failed to apply Put command", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to apply command: %v", err)
	}

	return &kv.PutResponse{Success: true}, nil
}

// Get implements kv.KVServer (stale reads)
func (h *KVHandler) Get(ctx context.Context, in *kv.GetRequest) (*kv.GetResponse, error) {
	h.logger.Info("Received Get request", zap.ByteString("key", in.GetKey()))
	var value []byte
	err := h.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(in.GetKey())
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		if err == badger.ErrKeyNotFound {
			return &kv.GetResponse{Found: false}, nil
		}
		h.logger.Error("Failed to get key", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to get value: %v", err)
	}
	return &kv.GetResponse{Value: value, Found: true}, nil
}

// Delete implements kv.KVServer
func (h *KVHandler) Delete(ctx context.Context, in *kv.DeleteRequest) (*kv.DeleteResponse, error) {
	h.logger.Info("Received Delete request", zap.ByteString("key", in.GetKey()))

	if h.raftNode.State() != raft.Leader {
		return nil, status.Errorf(codes.FailedPrecondition, "not leader")
	}

	cmd := command{Op: "DELETE", Key: in.GetKey()}
	cBytes, err := json.Marshal(cmd)
	if err != nil {
		h.logger.Error("Failed to marshal Delete command", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to marshal command: %v", err)
	}

	future := h.raftNode.Apply(cBytes, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		h.logger.Error("Failed to apply Delete command", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to apply command: %v", err)
	}

	return &kv.DeleteResponse{Success: true}, nil
}

// WriteBatch is not yet implemented with Raft
func (h *KVHandler) WriteBatch(ctx context.Context, in *kv.WriteBatchRequest) (*kv.WriteBatchResponse, error) {
	return nil, status.Error(codes.Unimplemented, "WriteBatch is not yet implemented")
}