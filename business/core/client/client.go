package client

import (
	"net/rpc"

	"github.com/google/uuid"
	meta "github.com/pyropy/dfs/business/core/chunk_metadata_service"
	"github.com/pyropy/dfs/business/rpc/master"
)

const ChunkSizeBytes = 64 * 10e+6

type Client struct {
	*meta.ChunkMetadataService

	rpcClient *rpc.Client
}

func NewClient(masterAddr string) (*Client, error) {
	rpcClient, err := rpc.DialHTTP("tcp", masterAddr)
	if err != nil {
		return nil, err
	}

	return &Client{
		rpcClient:            rpcClient,
		ChunkMetadataService: meta.NewChunkMetadataService(),
	}, nil
}

func (c *Client) CreateNewFile(path string, size int) (*master.CreateNewFileReply, error) {
	var reply master.CreateNewFileReply
	args := &master.CreateNewFileArgs{Path: path, Size: size}

	// TODO: Store chunk metadata
	err := c.rpcClient.Call("MasterAPI.CreateNewFile", args, &reply)
	if err != nil {
		return nil, err
	}

	// Chunks ids are generated sequentually hence we can index them by iterrating over chunk ids reply
	for chunkIndex, chunkId := range reply.Chunks {
		// TODO: Send initial chunk version from master
		chunkMetadata := meta.NewChunkMetadata(chunkId, chunkIndex, 1, reply.ChunkServerIDs)
		c.AddNewChunkMetadata(chunkMetadata)

	}

	return &reply, nil
}

func (c *Client) RequestChunkWrite(chunkID uuid.UUID) (*master.RequestWriteReply, error) {
	args := master.RequestWriteArgs{
		ChunkID: chunkID,
	}
	var reply master.RequestWriteReply
	err := c.rpcClient.Call("MasterAPI.RequestWrite", args, &reply)

	if err != nil {
		return nil, err
	}

	return &reply, nil
}

func (c *Client) WriteFile(path string, data []byte, offset int) {
	// TODO: calculate which chunk(s) will be written to by looking at offset and data len
	// TODO: Check if requesting creating new chunk if we dont have enough space in remaining chunks is needed?
}

func (c *Client) WriteChunk(chunkID uuid.UUID, data []byte, offset int) {

}
