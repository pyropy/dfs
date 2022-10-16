package main

import (
	"fmt"
	"github.com/pyropy/dfs/business/core/chunkserver"
	chunkServerRPC "github.com/pyropy/dfs/business/rpc/chunkserver"
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

// GrantLease ...
func (c *ChunkServerAPI) GrantLease(args *chunkServerRPC.GrantLeaseArgs, _ *chunkServerRPC.GrantLeaseReply) error {
	log.Println("ChunkServerAPI.GrantLease", args)

	err := c.ChunkServer.GrantLease(args.ChunkID, args.ValidUntil)
	if err != nil {
		return err
	}

	return nil
}

// IncrementChunkVersion ...
func (c *ChunkServerAPI) IncrementChunkVersion(args *chunkServerRPC.IncrementChunkVersionArgs, _ *chunkServerRPC.IncrementChunkVersionReply) error {
	log.Println("ChunkServerAPI.IncrementChunkVersion", args)

	return c.ChunkServer.IncrementChunkVersion(args.ChunkID, args.Version)
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

	err = chunkServer.RegisterChunkServer(MasterAddr, listenAddr)
	if err != nil {
		log.Println("startup", "error", "failed to RegisterChunkServer chunkserver")
		return err
	}

	// Start monitoring lease expiry
	go chunkServer.MonitorExpiredLeases()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
	log.Println("shutdown", "status", "chunkserver rpc server stopping", listenAddr)

	return nil
}
