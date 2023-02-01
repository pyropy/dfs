package main


import (
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
func (c *ChunkServerAPI) CreateChunk(args *chunkServerRPC.CreateChunkRequest, reply *chunkServerRPC.CreateChunkReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.CreateChunk", "args", args)
	chunk, err := c.ChunkServer.CreateChunk(args.ChunkID, args.FilePath, args.ChunkIndex, args.ChunkVersion, args.ChunkSize)
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
	log.Infow("rpc", "event", "ChunkServerAPI.GrantLease", "args", args)

	err := c.ChunkServer.GrantLease(args.ChunkID, args.ValidUntil)
	if err != nil {
		return err
	}

	return nil
}

// IncrementChunkVersion ...
func (c *ChunkServerAPI) IncrementChunkVersion(args *chunkServerRPC.IncrementChunkVersionArgs, _ *chunkServerRPC.IncrementChunkVersionReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.IncrementChunkVersion", "args", args)

	return c.ChunkServer.IncrementChunkVersion(args.ChunkID, args.Version)
}

func (c *ChunkServerAPI) TransferData(args *chunkServerRPC.TransferDataArgs, reply *chunkServerRPC.TransferDataReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.TransferData", "checksum", args.CheckSum)

	err := c.ChunkServer.ReceiveBytes(args.Data, args.CheckSum)
	if err != nil {
		return err
	}

	return nil
}

func (c *ChunkServerAPI) WriteChunk(args *chunkServerRPC.WriteChunkArgs, reply *chunkServerRPC.WriteChunkReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.WriteChunk", "args", args)

	bytesWritten, err := c.ChunkServer.WriteChunk(args.ChunkID, args.CheckSum, args.Offset, args.Version, args.ChunkServers)
	if err != nil {
		return err
	}

	reply.BytesWritten = bytesWritten

	return nil
}

func (c *ChunkServerAPI) ApplyMigration(args *chunkServerRPC.ApplyMigrationArgs, reply *chunkServerRPC.ApplyMigrationReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.ApplyMigration", "args", args)
	chunkServers := []chunkServerRPC.ChunkServer{}

	bytesWritten, err := c.ChunkServer.WriteChunk(args.ChunkID, args.CheckSum, args.Offset, args.Version, chunkServers)
	if err != nil {
		return err
	}

	reply.BytesWritten = bytesWritten

	return nil
}

func (c *ChunkServerAPI) ReplicateChunk(args *chunkServerRPC.ReplicateChunkArgs, reply *chunkServerRPC.ReplicateChunkReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.ReplicateChunk", "args", args)

	err := c.ChunkServer.ReplicateChunk(args.ChunkID, args.ChunkServers)
	if err != nil {
		return err
	}

	return nil
}

