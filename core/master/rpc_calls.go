package master

import (
	"net/rpc"

	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/model"
	csRpc "github.com/pyropy/dfs/rpc/chunkserver"
)

const (
	RpcCreateChunk           = "ChunkServerAPI.CreateChunk"
	RpcGrantLease            = "ChunkServerAPI.GrantLease"
	RpcIncrementChunkVersion = "ChunkServerAPI.IncrementChunkVersion"
	RpcDeleteChunk           = "ChunkServerAPI.DeleteChunk"
)

func createNewChunk(id uuid.UUID, filePath string, size int, chunkVersion int, chunkServer *ChunkServerMetadata) error {
	args := csRpc.CreateChunkRequest{
		ChunkID:      id,
		ChunkSize:    size,
		ChunkVersion: chunkVersion,
		FilePath:     filePath,
	}

	reply := csRpc.CreateChunkReply{}
	return call(chunkServer, RpcCreateChunk, args, &reply)
}

func sendLeaseGrant(chunkID uuid.UUID, lease *model.Lease, chunkServer *ChunkServerMetadata) error {
	args := csRpc.GrantLeaseArgs{
		ChunkID:    chunkID,
		ValidUntil: lease.ValidUntil,
	}

	reply := csRpc.GrantLeaseReply{}
	return call(chunkServer, RpcGrantLease, args, &reply)
}

func incrementChunkVersion(chunkID uuid.UUID, version int, chunkServer *ChunkServerMetadata) error {
	args := csRpc.IncrementChunkVersionArgs{
		ChunkID: chunkID,
		Version: version,
	}
	reply := csRpc.IncrementChunkVersionReply{}

	return call(chunkServer, RpcIncrementChunkVersion, args, &reply)
}

func deleteChunk(chunkID uuid.UUID, chunkServer *ChunkServerMetadata) error {
	args := csRpc.DeleteChunkRequest{
		ChunkID: chunkID,
	}
	reply := csRpc.DeleteChunkReply{}

	return call(chunkServer, RpcDeleteChunk, args, &reply)
}

func call(chunkServer *ChunkServerMetadata, method string, args interface{}, reply interface{}) error {
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
