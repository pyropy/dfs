package main

import (
	masterCore "github.com/pyropy/dfs/core/master"
	"github.com/pyropy/dfs/lib/logger"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
)

var log, _ = logger.New("master")

func main() {
	if err := run(); err != nil {
		log.Fatalln("startup", "ERROR", err)
	}
}

func run() error {
	master := masterCore.NewMaster()
	masterAPI := NewMasterAPI(master)

	rpc.Register(masterAPI)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Infow("startup", "error", "net listen failed")
		return err
	}

	log.Infow("startup", "status", "master rpc server started", "address", l.Addr().String())
	defer log.Infow("shutdown", "status", "master rpc server stopped", "address", l.Addr().String())
	go http.Serve(l, nil)

	log.Infow("startup", "status", "starting healtcheck")
	go master.StartHealthCheck()

	log.Infow("startup", "status", "starting replication monitor")
	go master.StartReplicationMonitor()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
	log.Infow("shutdown", "status", "master rpc server stopping", "address", l.Addr().String())

	return nil
}
