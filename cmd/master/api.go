package main

import (
	"github.com/pyropy/dfs/core/constants"
	core "github.com/pyropy/dfs/core/master"
	"github.com/pyropy/dfs/core/model"
	rpc "github.com/pyropy/dfs/rpc/master"
)

type API struct {
	server *core.Master
}

func NewMasterAPI(master *core.Master) *API {
	return &API{
		server: master,
	}
}

func (a *API) RegisterChunkServer(args *rpc.RegisterArgs, reply *rpc.RegisterReply) error {
	log.Infow("rpc", "event", "RegisterChunkServer", "args", args)
	chunkServer := a.server.RegisterNewChunkServer(args.Address)
	reply.ID = chunkServer.ID

	log.Infow("rpc", "status", "registered new chunk server", "id", chunkServer.ID, "address", chunkServer.Address)

	return nil
}

func (a *API) CreateNewFile(args *rpc.CreateNewFileArgs, reply *rpc.CreateNewFileReply) error {
	log.Infow("rpc", "event", "CreateNewFile", "args", args)
	file, chunkServerIds, err := a.server.CreateNewFile(args.Path, args.Size, constants.REPLICATION_FACTOR, constants.CHUNK_SIZE_BYTES)
	if err != nil {
		return err
	}

	reply.Chunks = file.Chunks
	reply.ChunkServerIDs = chunkServerIds
	return nil
}

func (a *API) DeleteFile(args *rpc.DeleteFileArgs, reply *rpc.DeleteFileReply) error {
	log.Infow("rpc", "event", "DeleteFile", "args", args)
	return nil
}

func (a *API) RequestLeaseRenewal(args *rpc.RequestLeaseRenewalArgs, reply *rpc.RequestLeaseRenewalReply) error {
	log.Infow("rpc", "event", "RequestLeaseRenewal", "args", args)
	chs := core.ChunkServerMetadata{
		ID: args.ChunkServerID,
	}

	lease, err := a.server.RequestLeaseRenewal(args.ChunkID, &chs)
	if err != nil {
		return err
	}

	reply.Granted = true
	reply.ChunkID = lease.ChunkID
	reply.ValidUntil = lease.ValidUntil
	return nil
}

func (a *API) RequestWrite(args *rpc.RequestWriteArgs, reply *rpc.RequestWriteReply) error {
	log.Infow("rpc", "event", "RequestWrite", "args", args)
	var chunkServers []rpc.ChunkServer
	chunkID, lease, chunkHolders, chunkVersion, err := a.server.RequestWrite(args.ChunkID)
	if err != nil {
		return err
	}

	log.Infow("request write", "chunkID", chunkID, "lease", lease, "chunkHolders", chunkHolders, "chunkVersion", chunkVersion, "err", err)

	for _, chunkHolder := range chunkHolders {
		chunkServer := rpc.ChunkServer{
			ID:      chunkHolder.ID,
			Address: chunkHolder.Address,
		}
		chunkServers = append(chunkServers, chunkServer)
	}

	reply.ChunkID = chunkID
	reply.PrimaryChunkServerID = lease.ChunkServerID
	reply.ValidUntil = lease.ValidUntil
	reply.ChunkServers = chunkServers
	reply.Version = chunkVersion

	return nil
}

// TODO: Catch stale chunks
func (a *API) ReportHealth(args *rpc.ReportHealthArgs, _ *rpc.ReportHealthReply) error {
	log.Infow("rpc", "event", "ReportHealth", "args", args)
	var chunks []model.ChunkMetadata
	// Map rpc Chunks to ChunkMetadata
	for _, c := range args.Chunks {
		metadata := model.ChunkMetadata{
			Chunk: model.Chunk{
				ID:      c.ID,
				Version: c.Version,
				Index:   c.Index,
			},
		}

		chunks = append(chunks, metadata)
	}

	a.server.MarkHealthy(args.ChunkServerID)
	a.server.UpdateChunksLocation(args.ChunkServerID, chunks)

	return nil
}
