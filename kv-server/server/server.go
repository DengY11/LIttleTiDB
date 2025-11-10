package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/your-username/mytidb/kv-server/fsm"
	"github.com/your-username/mytidb/kv-server/handler"
	pd "github.com/your-username/mytidb/kv-server/pd_client"
	kv "github.com/your-username/mytidb/kv-server/proto"
	pd_proto "github.com/your-username/mytidb/pd/proto"
	"github.com/your-username/mytidb/kv-server/store"
)

// Config is the configuration for the server.
type Config struct {
	Port         int    `yaml:"port"`
	RaftPort     int    `yaml:"raft_port"`
	PDAddr       string `yaml:"pd_addr"`
	LogLevel     string `yaml:"log_level"`
	DBPath       string `yaml:"db_path"`
	LogPath      string `yaml:"log_path"`
	SnapshotPath string `yaml:"snapshot_path"`
}

// Server is the main server for the kv-server.
type Server struct {
	config        *Config
	logger        *zap.Logger
	db            *badger.DB
	raftNode      *raft.Raft
	pdClient      *pd.PDClient
	grpcSrv       *grpc.Server
	lis           net.Listener
	raftStore     *store.BadgerStore
	snapshotStore raft.SnapshotStore
	transport     raft.Transport
	nodeID        uint64
}

// NewServer creates a new server.
func NewServer(config *Config, logger *zap.Logger) (*Server, error) {
	pdClient, err := pd.NewPDClient(config.PDAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PD: %w", err)
	}

	// Allocate a unique node ID from PD.
	nodeID, err := pdClient.AllocID(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to allocate node ID from PD: %w", err)
	}
	logger.Info("allocated node id from pd", zap.Uint64("node_id", nodeID))

	db, err := badger.Open(badger.DefaultOptions(config.DBPath).WithLogger(nil))
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	raftStore := store.NewBadgerStore(db)
	raftFSM := fsm.NewBadgerFSM(db)

	snapshotStore, err := raft.NewFileSnapshotStore(config.SnapshotPath, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	raftListenAddr := fmt.Sprintf("0.0.0.0:%d", config.RaftPort)
	advertise, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", config.RaftPort))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft advertise address: %w", err)
	}
	transport, err := raft.NewTCPTransport(raftListenAddr, advertise, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft transport: %w", err)
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(strconv.FormatUint(nodeID, 10))
	raftConfig.HeartbeatTimeout = 200 * time.Millisecond
	raftConfig.ElectionTimeout = 1000 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 200 * time.Millisecond

	raftNode, err := raft.NewRaft(raftConfig, raftFSM, raftStore, raftStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}

	grpcSrv := grpc.NewServer()
	kvHandler := handler.NewKVHandler(db, logger, raftNode)
	kv.RegisterKVServer(grpcSrv, kvHandler)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	return &Server{
		config:        config,
		logger:        logger,
		db:            db,
		raftNode:      raftNode,
		pdClient:      pdClient,
		grpcSrv:       grpcSrv,
		lis:           lis,
		raftStore:     raftStore,
		snapshotStore: snapshotStore,
		transport:     transport,
		nodeID:        nodeID,
	}, nil
}



// Start starts the server.
func (s *Server) Start() error {
	s.logger.Info("Starting server...")

	// Start gRPC server
	go func() {
		if err := s.grpcSrv.Serve(s.lis); err != nil {
			s.logger.Error("gRPC server failed", zap.Error(err))
		}
	}()

	// Bootstrap or join the cluster
	hasState, err := raft.HasExistingState(s.raftStore, s.raftStore, s.snapshotStore)
	if err != nil {
		return fmt.Errorf("failed to check for existing raft state: %w", err)
	}

	if !hasState {
		// This is a new node, it needs to either bootstrap or join.
		// Check if the cluster has been initialized by querying PD.
		_, _, err := s.pdClient.GetRegion(context.Background(), []byte(""))
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.NotFound {
				// PD has no region info, so we are the first node and should bootstrap.
				s.logger.Info("No existing state and no region found in PD, bootstrapping cluster")
				if err := s.bootstrapCluster(); err != nil {
					return fmt.Errorf("failed to bootstrap cluster: %w", err)
				}
			} else {
				// Some other error when contacting PD.
				return fmt.Errorf("failed to get region from PD: %w", err)
			}
		} else {
			// A region exists, so we should join the existing cluster.
			s.logger.Info("Node starting without state, attempting to join existing cluster")
			go s.joinCluster()
		}
	} else {
		s.logger.Info("Node starting with existing state")
	}

	s.logger.Info("Server started", zap.String("grpc_addr", s.lis.Addr().String()))

	// Start sending heartbeats to PD.
	go s.sendStoreHeartbeat()
	go s.sendRegionHeartbeat()

	return nil
}

func (s *Server) joinCluster() {
	for {
		time.Sleep(5 * time.Second)
		// 1. Get region and leader from PD
		_, leader, err := s.pdClient.GetRegion(context.Background(), []byte(""))
		if err != nil {
			s.logger.Warn("failed to get region from PD, retrying...", zap.Error(err))
			continue
		}
		if leader == nil || leader.GetClientAddr() == "" {
			s.logger.Warn("no leader found, retrying...")
			continue
		}

		// We found a leader, try to join.
		leaderAddr := leader.GetClientAddr()
		s.logger.Info("found leader, attempting to join", zap.String("leader_addr", leaderAddr))

		conn, err := grpc.Dial(leaderAddr, grpc.WithInsecure())
		if err != nil {
			s.logger.Warn("failed to connect to leader", zap.String("leader_addr", leaderAddr), zap.Error(err))
			continue
		}
		kvClient := kv.NewKVClient(conn)

		req := &kv.JoinRequest{
			NodeId:   s.nodeID,
			RaftAddr: fmt.Sprintf("127.0.0.1:%d", s.config.RaftPort),
		}

		if _, err := kvClient.Join(context.Background(), req); err != nil {
			s.logger.Warn("failed to join cluster, will retry", zap.Error(err))
			conn.Close()
			continue
		}

		s.logger.Info("successfully joined cluster!")
		conn.Close()
		return // Exit the join loop
	}
}

func (s *Server) sendStoreHeartbeat() {
	ticker := time.NewTicker(10 * time.Second) // Adjust the interval as needed
	defer ticker.Stop()

	for range ticker.C {
		// In a real implementation, we would collect real stats.
		lastIndex, _ := strconv.ParseUint(s.raftNode.Stats()["last_index"], 10, 64)
		stats := &pd_proto.StoreStats{
			StoreId:     s.nodeID,
			RegionCount: uint32(lastIndex),
		}

		req := &pd_proto.StoreHeartbeatRequest{Stats: stats}
		if _, err := s.pdClient.StoreHeartbeat(context.Background(), req); err != nil {
			s.logger.Warn("Failed to send store heartbeat to PD", zap.Error(err))
		}
	}
}

func (s *Server) sendRegionHeartbeat() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if s.raftNode.State() != raft.Leader {
			continue
		}

		// This is a simplified heartbeat for a single region.
		region, leader, err := s.pdClient.GetRegion(context.Background(), []byte(""))
		if err != nil {
			s.logger.Warn("failed to get region for heartbeat", zap.Error(err))
			continue
		}

		if _, err := s.pdClient.RegionHeartbeat(context.Background(), region, leader); err != nil {
			s.logger.Warn("failed to send heartbeat to PD", zap.Error(err))
		}
	}
}

// Stop stops the server.
func (s *Server) Stop() {
	s.logger.Info("Stopping server...")
	s.grpcSrv.GracefulStop()
	s.db.Close()
	s.logger.Info("Server stopped")
}

func (s *Server) bootstrapCluster() error {
	s.logger.Info("Cluster not found, bootstrapping new cluster")
	regionID, err := s.pdClient.AllocID(context.Background())
	if err != nil {
		return fmt.Errorf("failed to allocate region ID from PD: %w", err)
	}
	peerID, err := s.pdClient.AllocID(context.Background())
	if err != nil {
		return fmt.Errorf("failed to allocate peer ID from PD: %w", err)
	}

	bootstrapConfig := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(strconv.FormatUint(s.nodeID, 10)),
				Address: s.transport.LocalAddr(),
			},
		},
	}
	bootstrapFuture := s.raftNode.BootstrapCluster(bootstrapConfig)
	if err := bootstrapFuture.Error(); err != nil {
		return fmt.Errorf("failed to bootstrap cluster: %w", err)
	}

	// Send the first heartbeat synchronously to register the new cluster.
	s.logger.Info("Bootstrap successful, sending initial heartbeat to PD")
	region := &pd_proto.Region{
		Id:       regionID,
		StartKey: []byte(""),
		EndKey:   []byte(""), // Empty end key means infinity
	}
	leader := &pd_proto.Peer{
		Id:         peerID,
		StoreId:    s.nodeID,
		Addr:       string(s.transport.LocalAddr()),
		ClientAddr: s.lis.Addr().String(),
	}
	if _, err := s.pdClient.RegionHeartbeat(context.Background(), region, leader); err != nil {
		return fmt.Errorf("failed to send initial heartbeat to PD: %w", err)
	}
	s.logger.Info("Initial heartbeat sent to PD successfully")
	return nil
}