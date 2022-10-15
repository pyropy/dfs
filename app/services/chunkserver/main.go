package main

import (
	"fmt"
	"github.com/pyropy/dfs/business/core/chunkserver"
	chunkServerRPC "github.com/pyropy/dfs/business/rpc/chunkserver"
	"github.com/pyropy/dfs/business/rpc/master"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
)

var MasterAddr = "localhost:1234"
var ListenAddr = "localhost"
var ListenPort = 0

type ChunkServerAPI struct {
	ChunkServer *chunkserver.ChunkServer
}

func NewChunkServerAPI(chunkServer *chunkserver.ChunkServer) *ChunkServerAPI {
	return &ChunkServerAPI{
		ChunkServer: chunkServer,
	}
}

// HealthCheck ...
func (c *ChunkServerAPI) HealthCheck(_ *chunkServerRPC.HealthCheckArgs, reply *chunkServerRPC.HealthCheckReply) error {
	reply.Status = 200

	return nil
}

// CreateChunk ...
func (c *ChunkServerAPI) CreateChunk(request *chunkServerRPC.CreateChunkRequest, reply *chunkServerRPC.CreateChunkReply) error {
	log.Println("ChunkServerAPI.CreateChunk", request)
	chunk, err := c.ChunkServer.CreateChunk(request.ChunkID, request.ChunkVersion, request.ChunkSize)
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
	chunkServer := chunkserver.NewChunkServer()
	chunkServerAPI := NewChunkServerAPI(chunkServer)

	rpc.Register(chunkServerAPI)
	rpc.HandleHTTP()
	addr := fmt.Sprintf("%s:%d", ListenAddr, ListenPort)

	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Println("startup", "error", "net listen failed")
		return err
	}

	listenAddr := l.Addr().String()

	log.Println("startup", "status", "chunkserver rpc server started", listenAddr)
	defer log.Println("shutdown", "status", "chunkserver rpc server stopped", listenAddr)
	go http.Serve(l, nil)

	err = RegisterChunkServer(listenAddr)
	if err != nil {
		log.Println("startup", "error", "failed to RegisterChunkServer chunkserver")
		return err
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
	log.Println("shutdown", "status", "chunkserver rpc server stopping", listenAddr)

	return nil
}

// RegisterChunkServer registers chunk server instance with Master API
func RegisterChunkServer(addr string) error {
	client, err := rpc.DialHTTP("tcp", MasterAddr)
	if err != nil {
		log.Println("error", "unreachable")
		return err
	}

	var reply master.RegisterReply
	args := &master.RegisterArgs{Address: addr}
	err = client.Call("MasterAPI.RegisterChunkServer", args, &reply)
	if err != nil {
		return err
	}

	return nil
}
