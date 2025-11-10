package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/google/btree"
	"github.com/your-username/mytidb/pd/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// regionItem is an item in the B-Tree.
type regionItem struct {
	region *proto.Region
}

// Less implements the btree.Item interface.
func (r *regionItem) Less(than btree.Item) bool {
	return string(r.region.GetStartKey()) < string(than.(*regionItem).region.GetStartKey())
}

// Server implements the PD service.
type Server struct {
	proto.UnimplementedPDServer

	mu            sync.RWMutex
	regions       *btree.BTree
	stores        map[uint64]*proto.StoreStats
	leader        map[uint64]*proto.Peer // regionID -> leader peer
	logger        *zap.Logger
	snowflakeNode *snowflake.Node
}

// NewServer creates a new PD server.
func NewServer(logger *zap.Logger) (*Server, error) {
	node, err := snowflake.NewNode(1) // Use a fixed node ID for simplicity.
	if err != nil {
		logger.Error("Failed to create Snowflake node", zap.Error(err))
		return nil, err
	}

	return &Server{
		regions:       btree.New(2), // 2 is the degree of the B-Tree
		stores:        make(map[uint64]*proto.StoreStats),
		leader:        make(map[uint64]*proto.Peer),
		logger:        logger.Named("pd-server"),
		snowflakeNode: node,
	}, nil
}

// StoreHeartbeat is called by a store to report its state.
func (s *Server) StoreHeartbeat(ctx context.Context, req *proto.StoreHeartbeatRequest) (*proto.StoreHeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Received StoreHeartbeat", zap.Any("stats", req.Stats))
	s.stores[req.Stats.StoreId] = req.Stats

	// In a real implementation, PD would analyze the store stats and
	// might decide to schedule region transfers, etc. For now, we do nothing.

	return &proto.StoreHeartbeatResponse{}, nil
}

func (s *Server) LoggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	s.logger.Info("gRPC request", zap.String("method", info.FullMethod), zap.Any("request", req))

	resp, err := handler(ctx, req)

	duration := time.Since(start)
	if err != nil {
		s.logger.Error("gRPC request failed", zap.String("method", info.FullMethod), zap.Error(err), zap.Duration("duration", duration))
	} else {
		// Be careful logging responses, they can be large.
		// For this educational purpose, we'll truncate it.
		respStr := fmt.Sprintf("%v", resp)
		if len(respStr) > 100 {
			respStr = respStr[:100] + "..."
		}
		s.logger.Info("gRPC response", zap.String("method", info.FullMethod), zap.String("response", respStr), zap.Duration("duration", duration))
	}

	return resp, err
}

// GetRegion gets the region and its leader for a given key.
func (s *Server) GetRegion(ctx context.Context, req *proto.GetRegionRequest) (*proto.GetRegionResponse, error) {
	s.logger.Debug("Received GetRegion request", zap.ByteString("key", req.Key))
	s.mu.RLock()
	defer s.mu.RUnlock()

	searchItem := &regionItem{region: &proto.Region{StartKey: req.Key}}
	var result *regionItem

	s.regions.DescendLessOrEqual(searchItem, func(item btree.Item) bool {
		result = item.(*regionItem)
		return false // Stop after finding the first item
	})

	if result == nil || (len(result.region.GetEndKey()) > 0 && string(req.Key) >= string(result.region.GetEndKey())) {
		s.logger.Warn("Region not found for key", zap.ByteString("key", req.Key))
		return nil, status.Errorf(codes.NotFound, "region not found for key %s", req.Key)
	}

	leader := s.leader[result.region.Id]
	s.logger.Debug("Found region for key", zap.Uint64("region_id", result.region.Id), zap.Any("leader", leader))
	return &proto.GetRegionResponse{
		Region: result.region,
		Leader: leader,
	}, nil
}

func (s *Server) RegionHeartbeat(ctx context.Context, req *proto.RegionHeartbeatRequest) (*proto.RegionHeartbeatResponse, error) {
	s.logger.Info("received region heartbeat", zap.Any("region", req.Region), zap.Any("leader", req.Leader))
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store region info and update leader.
	s.regions.ReplaceOrInsert(&regionItem{region: req.Region})
	s.leader[req.Region.Id] = req.Leader

	return &proto.RegionHeartbeatResponse{}, nil
}

// AllocID allocates a new unique ID.
func (s *Server) AllocID(ctx context.Context, req *proto.AllocIDRequest) (*proto.AllocIDResponse, error) {
	id := s.snowflakeNode.Generate()
	s.logger.Info("Allocating new ID", zap.Int64("id", id.Int64()))
	return &proto.AllocIDResponse{Id: uint64(id.Int64())}, nil
}