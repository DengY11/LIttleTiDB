package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"

	"github.com/your-username/mytidb/pd/proto"
	"github.com/your-username/mytidb/pd/server"
)

func main() {
	// Initialize logger
	zapeConfig := zap.NewProductionEncoderConfig()
	zapeConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	consoleEncoder := zapcore.NewConsoleEncoder(zapeConfig)
	consoleCore := zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stderr), zap.DebugLevel)
	logger := zap.New(consoleCore)

	port := 2379 // Default port for PD, same as etcd
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	pdServer, err := server.NewServer(logger)
	if err != nil {
		logger.Fatal("Failed to create PD server", zap.Error(err))
	}
	s := grpc.NewServer(grpc.UnaryInterceptor(pdServer.LoggingInterceptor))
	proto.RegisterPDServer(s, pdServer)

	logger.Info("PD server listening", zap.String("address", lis.Addr().String()))
	if err := s.Serve(lis); err != nil {
		logger.Fatal("failed to serve", zap.Error(err))
	}
}