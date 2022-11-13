package client

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"sync"

	"github.com/google/uuid"
	meta "github.com/pyropy/dfs/business/core/chunk_metadata_service"
	"github.com/pyropy/dfs/business/core/constants"
	filemeta "github.com/pyropy/dfs/business/core/file_metadata_service"
	"github.com/pyropy/dfs/business/rpc/chunkserver"
	"github.com/pyropy/dfs/business/rpc/master"
)

const ChunkSizeBytes = 64 * 10e+6

var (
	ErrFileNotFound = errors.New("File not found.")
)

type Client struct {
	*meta.ChunkMetadataService
	*filemeta.FileMetadataService

	RpcClient *rpc.Client
}

func NewClient(masterAddr string) (*Client, error) {
	rpcClient, err := rpc.DialHTTP("tcp", masterAddr)
	if err != nil {
		return nil, err
	}

	return &Client{
		RpcClient:            rpcClient,
		ChunkMetadataService: meta.NewChunkMetadataService(),
		FileMetadataService:  filemeta.NewFileMetadataService(),
	}, nil
}

func (c *Client) CreateNewFile(path string, size int) (*master.CreateNewFileReply, error) {
	var reply master.CreateNewFileReply
	args := &master.CreateNewFileArgs{Path: path, Size: size}

	err := c.RpcClient.Call("MasterAPI.CreateNewFile", args, &reply)
	if err != nil {
		return nil, err
	}

	// Chunks ids are generated sequentually hence we can index them by iterrating over chunk ids reply
	for chunkIndex, chunkId := range reply.Chunks {
		chunkMetadata := meta.NewChunkMetadata(chunkId, chunkIndex, constants.INITIAL_CHUNK_VERSION, reply.ChunkServerIDs)
		c.AddNewChunkMetadata(chunkMetadata)

	}

	return &reply, nil
}

func (c *Client) RequestChunkWrite(chunkID uuid.UUID) (*master.RequestWriteReply, error) {
	args := master.RequestWriteArgs{
		ChunkID: chunkID,
	}
	var reply master.RequestWriteReply
	err := c.RpcClient.Call("MasterAPI.RequestWrite", args, &reply)

	if err != nil {
		return nil, err
	}

	return &reply, nil
}

func min(x, y int) int {
	if x < y {
		return x
	}

	return y
}

func (c *Client) WriteFile(path string, data *bytes.Buffer, offset int) (int, error) {
	fileMetadata := c.FileMetadataService.Get(path)
	if fileMetadata == nil {
		return 0, ErrFileNotFound
	}

	totalBytesWritten := 0
	remainingBytes := data.Len()
	chunkStartOffset := offset % constants.CHUNK_SIZE_BYTES

	log.Println("debug", "client", "WriteFile", "total bytes", data.Len())

	for chunkIdx := offset / constants.CHUNK_SIZE_BYTES; remainingBytes > 0; chunkIdx++ {
		log.Println("debug", "client", "WriteFile", "chunkIndex", chunkIdx, "remainingBytes", remainingBytes, "chunkStartOffset", chunkStartOffset)
		bytesToWrite := min(constants.CHUNK_SIZE_BYTES-chunkStartOffset, remainingBytes)
		b, err := ioutil.ReadAll(io.LimitReader(data, int64(bytesToWrite)))
		if err != nil {
			return totalBytesWritten, err
		}

		chunkId := fileMetadata.Chunks[chunkIdx]
		bytesWritten, err := c.WriteChunk(chunkId, b, chunkStartOffset)
		if err != nil {
			return totalBytesWritten, err
		}

		chunkStartOffset = 0
		remainingBytes -= bytesWritten
		totalBytesWritten += bytesWritten

	}

	// TODO: Check if requesting creating new chunk if we dont have enough space in remaining chunks is needed?
	return totalBytesWritten, nil
}

// WriteChunk sends request for write to master, pushes bytes to all chunk servers that hold copy of the chunk
// and sends request for write to chunk server that holds the lease granted by the master
func (c *Client) WriteChunk(chunkID uuid.UUID, data []byte, offset int) (int, error) {
	var bytesWritten int
	var wg sync.WaitGroup

	writeRequest, err := c.RequestChunkWrite(chunkID)
	if err != nil {
		return bytesWritten, err
	}

	// push bytes in parallel
	for _, cs := range writeRequest.ChunkServers {
		wg.Add(1)
		go func(chunkServer master.ChunkServer) {
			defer wg.Done()

			c.SendBytes(chunkServer.Address, data)
		}(cs)
	}

	wg.Wait()

	// find address of lease server
	var leaseAddr string
	for _, cs := range writeRequest.ChunkServers {
		if cs.ID == writeRequest.PrimaryChunkServerID {
			leaseAddr = cs.Address
		}
	}

	rpcClient, err := rpc.DialHTTP("tcp", leaseAddr)
	if err != nil {
		return 0, err
	}

	defer rpcClient.Close()

	// TODO: stupid cast, fix
	var chunkServers []chunkserver.ChunkServer
	for _, cs := range writeRequest.ChunkServers {
		chunkServer := chunkserver.ChunkServer(cs)
		chunkServers = append(chunkServers, chunkServer)
	}

	// TODO: Calculate checksum
	args := chunkserver.WriteChunkArgs{
		ChunkID:      chunkID,
		CheckSum:     123123,
		Offset:       offset,
		Version:      writeRequest.Version,
		ChunkServers: chunkServers,
	}

	var reply chunkserver.WriteChunkReply
	err = rpcClient.Call("ChunkServerAPI.WriteChunk", args, &reply)
	if err != nil {
		return 0, err
	}

	return reply.BytesWritten, nil
}

func (c *Client) SendBytes(addr string, data []byte) (*chunkserver.TransferDataReply, error) {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return nil, err
	}

	defer rpcClient.Close()

	args := chunkserver.TransferDataArgs{
		Data:     data,
		CheckSum: 123123,
	}

	var reply chunkserver.TransferDataReply
	err = rpcClient.Call("ChunkServerAPI.TransferData", args, &reply)

	if err != nil {
		return nil, err
	}

	return &reply, nil
}
