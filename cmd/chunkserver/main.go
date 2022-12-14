package main

import (
	"fmt"
	"github.com/pyropy/dfs/core/chunkserver"
	chunkServerRPC "github.com/pyropy/dfs/rpc/chunkserver"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
)

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
	chunk, err := c.ChunkServer.CreateChunk(request.ChunkID, request.FilePath, request.ChunkIndex, request.ChunkVersion, request.ChunkSize)
	if err != nil {
		return err
	}

	reply.ChunkID = chunk.ID
	reply.ChunkIndex = chunk.Index
	reply.ChunkVersion = chunk.Version

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

func (c *ChunkServerAPI) TransferData(args *chunkServerRPC.TransferDataArgs, reply *chunkServerRPC.TransferDataReply) error {
	log.Println("ChunkServerAPI.TransferData", args.CheckSum)

	err := c.ChunkServer.ReceiveBytes(args.Data, args.CheckSum)
	if err != nil {
		return err
	}

	return nil
}

func (c *ChunkServerAPI) WriteChunk(args *chunkServerRPC.WriteChunkArgs, reply *chunkServerRPC.WriteChunkReply) error {
	log.Println("ChunkServerAPI.WriteChunk", args)

	bytesWritten, err := c.ChunkServer.WriteChunk(args.ChunkID, args.CheckSum, args.Offset, args.Version, args.ChunkServers)
	if err != nil {
		return err
	}

	reply.BytesWritten = bytesWritten

	return nil
}

func (c *ChunkServerAPI) ApplyMigration(args *chunkServerRPC.ApplyMigrationArgs, reply *chunkServerRPC.ApplyMigrationReply) error {
	log.Println("ChunkServerAPI.ApplyMigration", args)
	chunkServers := []chunkServerRPC.ChunkServer{}

	bytesWritten, err := c.ChunkServer.WriteChunk(args.ChunkID, args.CheckSum, args.Offset, args.Version, chunkServers)
	if err != nil {
		return err
	}

	reply.BytesWritten = bytesWritten

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

	cfg, err := GetConfig()
	if err != nil {
		log.Println("startup", "error", "config error")
		return err
	}

	rpc.Register(chunkServerAPI)
	rpc.HandleHTTP()
	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)

	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Println("startup", "error", "net listen failed")
		return err
	}

	listenAddr := l.Addr().String()

	log.Println("startup", "status", "chunkserver rpc server started", listenAddr)
	defer log.Println("shutdown", "status", "chunkserver rpc server stopped", listenAddr)
	go http.Serve(l, nil)

	err = chunkServer.RegisterChunkServer(cfg.Master.Addr, listenAddr)
	if err != nil {
		log.Println("startup", "error", "failed to RegisterChunkServer chunkserver")
		return err
	}

	// Start monitoring lease expiry
	go chunkServer.MonitorExpiredLeases()

	// Start reporting health to master
	go chunkServer.StartHealthReport()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
	log.Println("shutdown", "status", "chunkserver rpc server stopping", listenAddr)

	return nil
}
