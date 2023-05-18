package main

import (
	"github.com/pyropy/dfs/core/constants"
	masterCore "github.com/pyropy/dfs/core/master"
	"github.com/pyropy/dfs/core/model"
	masterRPC "github.com/pyropy/dfs/rpc/master"
)


type MasterAPI struct {
	Master *masterCore.Master
}

func NewMasterAPI(master *masterCore.Master) *MasterAPI {
	return &MasterAPI{
		Master: master,
	}
}

func (m *MasterAPI) RegisterChunkServer(args *masterRPC.RegisterArgs, reply *masterRPC.RegisterReply) error {
	log.Infow("rpc", "event", "RegisterChunkServer", "args", args)
	chunkServer := m.Master.RegisterNewChunkServer(args.Address)
	reply.ID = chunkServer.ID

	log.Infow("rpc", "status", "registered new chunk server", "id", chunkServer.ID, "address", chunkServer.Address)

	return nil
}

func (m *MasterAPI) CreateNewFile(args *masterRPC.CreateNewFileArgs, reply *masterRPC.CreateNewFileReply) error {
	log.Infow("rpc", "event", "CreateNewFile", "args", args)
	file, chunkServerIds, err := m.Master.CreateNewFile(args.Path, args.Size, constants.REPLICATION_FACTOR, constants.CHUNK_SIZE_BYTES)
	if err != nil {
		return err
	}

	reply.Chunks = file.Chunks
	reply.ChunkServerIDs = chunkServerIds
	return nil
}

func (m *MasterAPI) RequestLeaseRenewal(args *masterRPC.RequestLeaseRenewalArgs, reply *masterRPC.RequestLeaseRenewalReply) error {
	log.Infow("rpc", "event", "RequestLeaseRenewal", "args", args)
	chs := masterCore.ChunkServerMetadata{
		ID: args.ChunkServerID,
	}

	lease, err := m.Master.RequestLeaseRenewal(args.ChunkID, &chs)
	if err != nil {
		return err
	}

	reply.Granted = true
	reply.ChunkID = lease.ChunkID
	reply.ValidUntil = lease.ValidUntil
	return nil
}

func (m *MasterAPI) RequestWrite(args *masterRPC.RequestWriteArgs, reply *masterRPC.RequestWriteReply) error {
	log.Infow("rpc", "event", "RequestWrite", "args", args)
	chunkServers := []masterRPC.ChunkServer{}
	lease, chunkVersion, err := m.Master.RequestWrite(args.ChunkID)
	if err != nil {
		return err
	}

	chunkHoldersIDs := m.Master.GetChunkHolders(args.ChunkID)
	for _, chunkHolderID := range chunkHoldersIDs {
		chunkHolder := m.Master.GetChunkServerMetadata(chunkHolderID)
		chunkServer := masterRPC.ChunkServer{
			ID:      chunkHolder.ID,
			Address: chunkHolder.Address,
		}
		chunkServers = append(chunkServers, chunkServer)
	}

	reply.ChunkID = args.ChunkID
	reply.PrimaryChunkServerID = lease.ChunkServerID
	reply.ValidUntil = lease.ValidUntil
	reply.ChunkServers = chunkServers
	reply.Version = chunkVersion

	return nil
}

// TODO: Catch stale chunks
func (m *MasterAPI) ReportHealth(args *masterRPC.ReportHealthArgs, _ *masterRPC.ReportHealthReply) error {
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

	m.Master.MarkHealthy(args.ChunkServerID)
	m.Master.UpdateChunksLocation(args.ChunkServerID, chunks)

	return nil
}

