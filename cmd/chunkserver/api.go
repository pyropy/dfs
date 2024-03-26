package main

import (
	core "github.com/pyropy/dfs/core/chunkserver"
	rpc "github.com/pyropy/dfs/rpc/chunkserver"
)

type API struct {
	server *core.ChunkServer
}

func NewChunkServerAPI(chunkServer *core.ChunkServer) *API {
	return &API{
		server: chunkServer,
	}
}

// CreateChunk ...
func (a *API) CreateChunk(args *rpc.CreateChunkRequest, reply *rpc.CreateChunkReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.CreateChunk", "args", args)
	chunk, err := a.server.CreateChunk(args.ChunkID, args.FilePath, args.ChunkIndex, args.ChunkVersion, args.ChunkSize)
	if err != nil {
		return err
	}

	reply.ChunkID = chunk.ID
	reply.ChunkIndex = chunk.Index
	reply.ChunkVersion = chunk.Version

	return nil
}

// DeleteChunk ...
func (a *API) DeleteChunk(args *rpc.DeleteChunkRequest, _ *rpc.DeleteChunkReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.DeleteChunk", "args", args)
	return a.server.DeleteChunk(args.ChunkID)
}

// GrantLease ...
func (a *API) GrantLease(args *rpc.GrantLeaseArgs, _ *rpc.GrantLeaseReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.GrantLease", "args", args)

	err := a.server.GrantLease(args.ChunkID, args.ValidUntil)
	if err != nil {
		return err
	}

	return nil
}

// IncrementChunkVersion ...
func (a *API) IncrementChunkVersion(args *rpc.IncrementChunkVersionArgs, _ *rpc.IncrementChunkVersionReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.IncrementChunkVersion", "args", args)

	return a.server.IncrementChunkVersion(args.ChunkID, args.Version)
}

func (a *API) TransferData(args *rpc.TransferDataArgs, _ *rpc.TransferDataReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.TransferData", "checksum", args.CheckSum)

	err := a.server.ReceiveBytes(args.Data, args.CheckSum)
	if err != nil {
		return err
	}

	return nil
}

func (a *API) WriteChunk(args *rpc.WriteChunkArgs, reply *rpc.WriteChunkReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.WriteChunk", "args", args)

	bytesWritten, err := a.server.WriteChunk(args.ChunkID, args.CheckSum, args.Offset, args.Version, args.ChunkServers)
	if err != nil {
		return err
	}

	reply.BytesWritten = bytesWritten

	return nil
}

func (a *API) ApplyMigration(args *rpc.ApplyMigrationArgs, reply *rpc.ApplyMigrationReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.ApplyMigration", "args", args)

	bytesWritten, err := a.server.ApplyMigration(args.ChunkID, args.CheckSum, args.Offset, args.Version)
	if err != nil {
		return err
	}

	reply.BytesWritten = bytesWritten

	return nil
}

func (a *API) ReplicateChunk(args *rpc.ReplicateChunkArgs, reply *rpc.ReplicateChunkReply) error {
	log.Infow("rpc", "event", "ChunkServerAPI.ReplicateChunk", "args", args)

	err := a.server.ReplicateChunk(args.ChunkID, args.ChunkServers)
	if err != nil {
		return err
	}

	return nil
}
