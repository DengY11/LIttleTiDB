package main

import (
	"context"
	"flag"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	kv "github.com/your-username/mytidb/kv-server/proto"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "the address to connect to")
	flag.Parse()

	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := kv.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Put a key-value pair
	key := []byte("nihao")
	value := []byte("shijie")
	putResp, err := c.Put(ctx, &kv.PutRequest{Key: key, Value: value})
	if err != nil {
		log.Printf("could not put: %v", err)
	} else {
		log.Printf("Put success: %v", putResp.Success)
	}

	// Get the value back
	getResp, err := c.Get(ctx, &kv.GetRequest{Key: key})
	if err != nil {
		log.Fatalf("could not get: %v", err)
	}
	log.Printf("Get value: %s", getResp.Value)
}
