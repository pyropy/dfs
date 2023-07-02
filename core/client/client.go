package client

import (
	"bytes"
	"context"
	"errors"
	"github.com/pyropy/dfs/lib/checksum"
	"io"
	"io/ioutil"
	"net/rpc"
	"sync"

	"github.com/pyropy/dfs/core/constants"
	"github.com/pyropy/dfs/lib/logger"
	"github.com/pyropy/dfs/rpc/chunkserver"
	"github.com/pyropy/dfs/rpc/master"

	"github.com/google/uuid"
)

var log, _ = logger.New("client")

const ChunkSizeBytes = 64 * 10e+6

var (
	ErrFileNotFound = errors.New("file not found.")
)

type Client struct {
	*ChunkMetadataStore
	*FileMetadataStore

	RpcClient *rpc.Client
}

func NewClient(masterAddr string, dsPath string) (*Client, error) {
	rpcClient, err := rpc.DialHTTP("tcp", masterAddr)
	if err != nil {
		return nil, err
	}

	chunkMetadataService, err := NewChunkMetadataStore()
	if err != nil {
		return nil, err
	}

	fileMetadataService, err := NewFileMetadataStore(dsPath)
	if err != nil {
		return nil, err
	}

	return &Client{
		RpcClient:          rpcClient,
		ChunkMetadataStore: chunkMetadataService,
		FileMetadataStore:  fileMetadataService,
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
		chunkMetadata := NewChunkMetadata(chunkId, chunkIndex, constants.INITIAL_CHUNK_VERSION, reply.ChunkServerIDs)
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

func (c *Client) WriteFile(ctx context.Context, path string, data *bytes.Buffer, offset int) (int, error) {
	fileMetadata, err := c.FileMetadataStore.Get(ctx, path)
	if err != nil {
		return 0, err
	}

	if fileMetadata == nil {
		return 0, ErrFileNotFound
	}

	totalBytesWritten := 0
	remainingBytes := data.Len()
	chunkStartOffset := offset % constants.CHUNK_SIZE_BYTES

	log.Infow("WriteFile", "total bytes", data.Len())

	for chunkIdx := offset / constants.CHUNK_SIZE_BYTES; remainingBytes > 0; chunkIdx++ {
		log.Infow("WriteFile", "chunkIndex", chunkIdx, "remainingBytes", remainingBytes, "chunkStartOffset", chunkStartOffset)
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

    log.Infow("starting pushing data to chunk servers", "numChunkservers", len(writeRequest.ChunkServers), "lenBytes", len(data))
	// push bytes in parallel
	for _, cs := range writeRequest.ChunkServers {
		wg.Add(1)
		go func(chunkServer master.ChunkServer) {
			defer wg.Done()
			resp, err := c.SendBytes(chunkServer.Address, data)
			log.Infow("send bytes response", "bytesReceived", resp.NumBytesReceived, "err", err)
		}(cs)
	}

	wg.Wait()

    log.Infow("Finished with pushing data to servers")

	// find address of lease server
	var leaseAddr string
	for _, cs := range writeRequest.ChunkServers {
		if cs.ID == writeRequest.PrimaryChunkServerID {
			leaseAddr = cs.Address
            break
		}
	}

    log.Infow("Found lease addr", "leaseAddr", leaseAddr)

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



	checkSum := checksum.CalculateCheckSum(data)
    log.Infow("Performing write", "checkSum", checkSum)
	args := chunkserver.WriteChunkArgs{
		ChunkID:      chunkID,
		CheckSum:     checkSum,
		Offset:       offset,
		Version:      writeRequest.Version,
		ChunkServers: chunkServers,
	}

	var reply chunkserver.WriteChunkReply
	err = rpcClient.Call("ChunkServerAPI.WriteChunk", args, &reply)
	if err != nil {
		return 0, err
	}
    log.Infow("Bytes written")

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
		CheckSum: checksum.CalculateCheckSum(data),
	}

	var reply chunkserver.TransferDataReply
	err = rpcClient.Call("ChunkServerAPI.TransferData", args, &reply)

	if err != nil {
		return nil, err
	}

	return &reply, nil
}
