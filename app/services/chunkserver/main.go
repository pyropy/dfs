package main

import (
	"github.com/pyropy/dfs/business/core"
	rpcCore "github.com/pyropy/dfs/business/rpc"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
)

var MASTER_ADDR = "localhost:1234"

type ChunkServerAPI struct {
	ChunkServer *core.ChunkServer
}

func (c *ChunkServerAPI) HealthCheck(_ *rpcCore.HealthCheckArgs, reply *rpcCore.HealthCheckReply) error {
	reply.Status = 200
	return nil
}

func (c *ChunkServerAPI) CreateChunk(request *rpcCore.CreateChunkRequest, reply *rpcCore.CreateChunkReply) error {
	chunk, err := c.ChunkServer.CreateChunk(request.ChunkID, request.ChunkVersion)
	if err != nil {
		return err
	}
	reply.ChunkID = chunk.ID

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalln("startup", "ERROR", err)
	}
}

func run() error {
	chunkServer := new(core.ChunkServer)
	chunkServerAPI := ChunkServerAPI{
		ChunkServer: chunkServer,
	}
	rpc.Register(&chunkServerAPI)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		log.Println("startup", "error", "net listen failed")
		return err
	}

	listenAddr := l.Addr().String()

	log.Println("startup", "status", "chunkserver rpc server started", listenAddr)
	defer log.Println("shutdown", "status", "chunkserver rpc server stopped", listenAddr)
	go http.Serve(l, nil)

	err = register(listenAddr)
	if err != nil {
		log.Println("startup", "error", "failed to register chunkserver")
		return err
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
	log.Println("shutdown", "status", "chunkserver rpc server stopping", listenAddr)

	return nil
}

func register(addr string) error {
	client, err := rpc.DialHTTP("tcp", MASTER_ADDR)
	if err != nil {
		log.Println("error", "unreachable")
		return err
	}

	var reply rpcCore.RegisterReply
	args := &rpcCore.RegisterArgs{Address: addr}
	err = client.Call("MasterAPI.RegisterChunkServer", args, &reply)
	if err != nil {
		return err
	}

	return nil
}
