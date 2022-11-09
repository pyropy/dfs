package client

import (
	"net/rpc"

	"github.com/google/uuid"
	"github.com/pyropy/dfs/business/rpc/master"
)

type Client struct {
	rpcClient *rpc.Client
}

func NewClient(masterAddr string) (*Client, error) {
	rpcClient, err := rpc.DialHTTP("tcp", masterAddr)
	if err != nil {
		return nil, err
	}

	return &Client{
		rpcClient: rpcClient,
	}, nil
}

func (c *Client) CreateNewFile(path string, size int) (*master.CreateNewFileReply, error) {
	var reply master.CreateNewFileReply
	args := &master.CreateNewFileArgs{
		Path: path,
		Size: size,
	}

	err := c.rpcClient.Call("MasterAPI.CreateNewFile", args, &reply)
	if err != nil {
		return nil, err
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
