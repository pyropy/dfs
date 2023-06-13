package main

import (
	"fmt"
	"github.com/pyropy/dfs/lib/logger"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"

	"github.com/pyropy/dfs/core/chunkserver"
)

var log, _ = logger.New("chunk-server-rpc")

func main() {
	if err := run(); err != nil {
		log.Fatalln("startup", "ERROR", err)
	}
}

func run() error {
	//ctx, cancel := context.WithCancel(context.Background())
	chunkServer := chunkserver.NewChunkServer()
	chunkServerAPI := NewChunkServerAPI(chunkServer)

	cfg, err := GetConfig()
	if err != nil {
		log.Errorw("startup", "error", "config error")
		return err
	}

	rpc.Register(chunkServerAPI)
	rpc.HandleHTTP()
	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)

	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Errorw("startup", "error", "net listen failed")
		return err
	}

	listenAddr := l.Addr().String()

	log.Infow("startup", "status", "chunkserver rpc server started", "address", listenAddr)
	defer log.Infow("shutdown", "status", "chunkserver rpc server stopped", "address", listenAddr)
	go http.Serve(l, nil)

	err = chunkServer.RegisterChunkServer(cfg.Master.Addr, listenAddr)
	if err != nil {
		log.Errorw("startup", "error", "failed to RegisterChunkServer chunkserver")
		return err
	}

	// Start monitoring lease expiry
	go chunkServer.MonitorExpiredLeases()

	// Start reporting health to master
	go chunkServer.StartHealthReport()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
	log.Infow("shutdown", "status", "chunkserver rpc server stopping", "address", listenAddr)

	return nil
}
