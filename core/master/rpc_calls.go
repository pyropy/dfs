package master

import (
	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/model"
	chunkServerRPC "github.com/pyropy/dfs/rpc/chunkserver"
	"net/rpc"
)

func createNewChunk(id uuid.UUID, filePath string, size int, chunkVersion int, chunkServer *ChunkServerMetadata) error {
	args := chunkServerRPC.CreateChunkRequest{
		ChunkID:      id,
		ChunkSize:    size,
		ChunkVersion: chunkVersion,
		FilePath:     filePath,
	}

	reply := chunkServerRPC.CreateChunkReply{}
	return callChunkServerRPC(chunkServer, "ChunkServerAPI.CreateChunk", args, &reply)
}

func sendLeaseGrant(chunkID uuid.UUID, lease *model.Lease, chunkServer *ChunkServerMetadata) error {
	args := chunkServerRPC.GrantLeaseArgs{
		ChunkID:    chunkID,
		ValidUntil: lease.ValidUntil,
	}

	reply := chunkServerRPC.GrantLeaseReply{}
	return callChunkServerRPC(chunkServer, "ChunkServerAPI.GrantLease", args, &reply)
}

func incrementChunkVersion(chunkID uuid.UUID, version int, chunkServer *ChunkServerMetadata) error {
	args := chunkServerRPC.IncrementChunkVersionArgs{
		ChunkID: chunkID,
		Version: version,
	}
	reply := chunkServerRPC.IncrementChunkVersionReply{}

	return callChunkServerRPC(chunkServer, "ChunkServerAPI.IncrementChunkVersion", args, &reply)
}

func deleteChunk(chunkID uuid.UUID, chunkServer *ChunkServerMetadata) error {
	args := chunkServerRPC.DeleteChunkRequest{
		ChunkID: chunkID,
	}
	reply := chunkServerRPC.DeleteChunkReply{}

	return callChunkServerRPC(chunkServer, "ChunkServerAPI.DeleteChunk", args, &reply)
}

func callChunkServerRPC(chunkServer *ChunkServerMetadata, method string, args interface{}, reply interface{}) error {
	client, err := rpc.DialHTTP("tcp", chunkServer.Address)
	if err != nil {
		log.Info("error", chunkServer.Address, "unreachable")
		return err
	}

	defer client.Close()

	err = client.Call(method, args, reply)
	if err != nil {
		log.Info("error", chunkServer.Address, "error", err)
		return err
	}

	return nil
}
