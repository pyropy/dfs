package main

import (
	"fmt"
	"github.com/pyropy/dfs/lib/logger"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"

	"github.com/pyropy/dfs/core/chunkserver"
	chunkServerRPC "github.com/pyropy/dfs/rpc/chunkserver"
)

type ChunkServerAPI struct {
	ChunkServer *chunkserver.ChunkServer
}

func NewChunkServerAPI(chunkServer *chunkserver.ChunkServer) *ChunkServerAPI {
	return &ChunkServerAPI{
		ChunkServer: chunkServer,
	}
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

	log, err := logger.New("chunk-server-rpc")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

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

	log.Infow("startup", "status", "chunkserver rpc server started", listenAddr)
	defer log.Infow("shutdown", "status", "chunkserver rpc server stopped", listenAddr)
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
	log.Infow("shutdown", "status", "chunkserver rpc server stopping", listenAddr)

	return nil
}
