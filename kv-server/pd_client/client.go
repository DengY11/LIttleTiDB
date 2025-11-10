package client

import (
	"context"
	"time"

	"github.com/your-username/mytidb/pd/proto"
	"google.golang.org/grpc"
)

// PDClient is a client for PD.
type PDClient struct {
	proto.PDClient
}

// NewPDClient creates a new PDClient.
func NewPDClient(addr string) (*PDClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return &PDClient{proto.NewPDClient(conn)}, nil
}

// GetRegion gets the region and its leader for a given key.
func (c *PDClient) GetRegion(ctx context.Context, key []byte) (*proto.Region, *proto.Peer, error) {
	resp, err := c.PDClient.GetRegion(ctx, &proto.GetRegionRequest{Key: key})
	if err != nil {
		return nil, nil, err
	}
	return resp.Region, resp.Leader, nil
}

// RegionHeartbeat sends a heartbeat to PD.
func (c *PDClient) RegionHeartbeat(ctx context.Context, region *proto.Region, leader *proto.Peer) (*proto.RegionHeartbeatResponse, error) {
	req := &proto.RegionHeartbeatRequest{
		Region: region,
		Leader: leader,
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return c.PDClient.RegionHeartbeat(ctx, req)
}

// AllocID allocates a new unique ID.
func (c *PDClient) AllocID(ctx context.Context) (uint64, error) {
	req := &proto.AllocIDRequest{}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := c.PDClient.AllocID(ctx, req)
	if err != nil {
		return 0, err
	}
	return resp.Id, nil
}