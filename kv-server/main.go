package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"

	"github.com/your-username/mytidb/kv-server/server"
)

func main() {
	// Parse command-line flags
	configFile := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	// Load configuration
	configData, err := os.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("failed to read config file: %v", err)
	}
	var config server.Config
	err = yaml.Unmarshal(configData, &config)
	if err != nil {
		log.Fatalf("failed to unmarshal config: %v", err)
	}

	// Initialize logger
	zapeConfig := zap.NewProductionEncoderConfig()
	zapeConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	consoleEncoder := zapcore.NewConsoleEncoder(zapeConfig)
	consoleCore := zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stderr), zap.InfoLevel)
	logger := zap.New(consoleCore)

	// Create and start the server
	srv, err := server.NewServer(&config, logger)
	if err != nil {
		logger.Fatal("failed to create server", zap.Error(err))
	}

	if err := srv.Start(); err != nil {
		logger.Fatal("failed to start server", zap.Error(err))
	}

	// Wait for termination signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	srv.Stop()
}
