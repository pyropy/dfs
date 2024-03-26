package chunkserver

import (
	"context"
	"errors"
	"github.com/pyropy/dfs/core/model"
	"github.com/pyropy/dfs/lib/cache"
	"github.com/pyropy/dfs/lib/checksum"
	rpcChunkServer "github.com/pyropy/dfs/rpc/chunkserver"
	"github.com/pyropy/dfs/rpc/master"
	"log"
	"net/rpc"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ChunkServer struct {
	*ChunkService
	*LeaseStore
	*HealthMonitorService
	*LeaseMonitorService

	Cfg           *Config
	LRU           *cache.LRU
	MasterAddr    string
	ChunkServerID uuid.UUID
}

var (
	ErrChunkAlreadyExists   = errors.New("chunk already exists")
	ErrChunkVersionMismatch = errors.New("chunk version mismatch")
	ErrChunkLeaseNotGranted = errors.New("chunk lease not granted")
	ErrDataNotFoundInCache  = errors.New("data not found in cache")
	ErrChunkLeaseNotFound   = errors.New("chunk server does not have active lease on chunk")
	ErrChecksumNotMatching  = errors.New("given checksum does not match calculated checksum")
)

func NewChunkServer(cfg *Config) *ChunkServer {
	leaseExpChan := make(chan model.Lease)
	leaseStore := NewLeaseStore()
	chunkService := NewChunkService(cfg)

	return &ChunkServer{
		Cfg:                  cfg,
		LeaseStore:           leaseStore,
		ChunkService:         chunkService,
		LRU:                  cache.NewLRU(100),
		HealthMonitorService: NewHealthReportService(chunkService),
		LeaseMonitorService:  NewLeaseMonitor(leaseStore, leaseExpChan),
	}
}

func (c *ChunkServer) CreateChunk(id uuid.UUID, filePath string, index, version, size int) (*model.Chunk, error) {
	existingChunk, exists := c.GetChunk(id)
	if exists && existingChunk.Version == version {
		return nil, ErrChunkAlreadyExists
	}

	chunk, err := c.ChunkService.CreateChunk(id, filePath, index, version, size)
	if err != nil {
		return nil, err
	}

	return chunk, nil
}

func (c *ChunkServer) WriteChunk(chunkID uuid.UUID, checksum int, offset int, version int, chunkHolders []rpcChunkServer.ChunkServer) (int, error) {
	if !c.HasLease(chunkID) {
		return 0, ErrChunkLeaseNotFound
	}

	bytesWritten, err := c.ApplyMigration(chunkID, checksum, offset, version)
	if err != nil {
		return 0, err
	}

	// Notify other holders to apply migration
	var wg sync.WaitGroup
	for _, ch := range chunkHolders {
		// don't send migration to self
		if ch.ID == c.ChunkServerID {
			continue
		}

		wg.Add(1)
		go func(chunkServer rpcChunkServer.ChunkServer) {
			defer wg.Done()

			err = c.SendApplyMigration(chunkID, checksum, offset, version, chunkServer.Address)
			if err != nil {
				log.Println("error", "chunkServer", "failed to send apply migration", err)
			}
		}(ch)
	}

	wg.Wait()
	return bytesWritten, nil
}

func (c *ChunkServer) ApplyMigration(chunkID uuid.UUID, checksum int, offset int, version int) (int, error) {
	_, chunkExists := c.ChunkService.GetChunk(chunkID)
	if !chunkExists {
		return 0, ErrChunkDoesNotExist
	}

	data, exists := c.LRU.Get(checksum)
	if !exists {
		return 0, ErrDataNotFoundInCache
	}

	bytesWritten, err := c.WriteChunkBytes(chunkID, data, offset, version)
	if err != nil {
		return bytesWritten, err
	}

	return bytesWritten, nil
}

func (c *ChunkServer) DeleteChunk(chunkID uuid.UUID) error {
	c.LeaseStore.RemoveLease(chunkID)
	return c.ChunkService.DeleteChunk(chunkID)
}

func (c *ChunkServer) GrantLease(chunkID uuid.UUID, validUntil time.Time) error {
	_, exists := c.GetChunk(chunkID)
	if !exists {
		return ErrChunkDoesNotExist
	}

	c.LeaseStore.GrantLease(chunkID, validUntil)

	return nil
}

func (c *ChunkServer) ReceiveBytes(data []byte, inChecksum int) error {
	calculatedCheckSum := checksum.CalculateCheckSum(data)
	if calculatedCheckSum != inChecksum {
		return ErrChecksumNotMatching
	}

	c.LRU.Put(inChecksum, data)
	return nil
}

// IncrementChunkVersion increments chunk version number but also checks if
// there is a mismatch between version given by master and local chunk version
func (c *ChunkServer) IncrementChunkVersion(chunkID uuid.UUID, version int) error {
	return c.ChunkService.IncrementChunkVersion(chunkID, version)
}

func (c *ChunkServer) StartLeaseMonitor(ctx context.Context) {
	c.LeaseMonitorService.Start(ctx)
}

func (c *ChunkServer) StartHealthReport(ctx context.Context) {
	c.HealthMonitorService.Start(ctx)
}

// RegisterChunkServer registers chunk server instance with Master API
func (c *ChunkServer) RegisterChunkServer(masterAddr, addr string) error {
	client, err := rpc.DialHTTP("tcp", masterAddr)
	if err != nil {
		log.Println("error", "unreachable")
		return err
	}

	c.MasterAddr = masterAddr
	c.HealthMonitorService.masterAddr = masterAddr
	c.LeaseMonitorService.masterAddr = masterAddr

	var reply master.RegisterReply
	args := &master.RegisterArgs{Address: addr}
	err = client.Call("MasterAPI.RegisterChunkServer", args, &reply)
	if err != nil {
		return err
	}

	c.ChunkServerID = reply.ID
	c.HealthMonitorService.chunkServerID = reply.ID
	c.LeaseMonitorService.chunkServerID = reply.ID

	return nil
}

func (c *ChunkServer) SendApplyMigration(chunkID uuid.UUID, checksum int, offset int, version int, address string) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Println("error", "unreachable")
		return err
	}

	defer client.Close()

	var reply rpcChunkServer.ApplyMigrationReply
	args := &rpcChunkServer.ApplyMigrationArgs{
		ChunkID:  chunkID,
		CheckSum: checksum,
		Offset:   offset,
		Version:  version,
	}

	err = client.Call("ChunkServerAPI.ApplyMigration", args, &reply)
	if err != nil {
		return err
	}

	return nil
}

// TODO: Refactor
// ReplicateChunk replicates chunk with chunkID to list of provided chunkServers
func (c *ChunkServer) ReplicateChunk(chunkID uuid.UUID, chunkServers []rpcChunkServer.ChunkServer) error {
	chunk, exists := c.ChunkService.GetChunk(chunkID)
	if !exists {
		return ErrChunkDoesNotExist
	}

	// read data
	data, err := c.ChunkService.ReadChunk(chunkID, 0, -1)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	// create chunks
	for _, chunkServer := range chunkServers {
		wg.Add(1)
		go func(chunkServer rpcChunkServer.ChunkServer) error {
			defer wg.Done()

			conn, err := rpc.DialHTTP("tcp", chunkServer.Address)
			if err != nil {
				return err
			}

			defer conn.Close()

			// create chunk
			createChunkArgs := rpcChunkServer.CreateChunkRequest{
				ChunkID:      chunk.ID,
				ChunkVersion: chunk.Version,
				ChunkIndex:   chunk.Index,
				FilePath:     chunk.FilePath,
			}

			var createChunkReply rpcChunkServer.CreateChunkReply

			err = conn.Call("ChunkServerAPI.CreateChunk", createChunkArgs, &createChunkReply)
			if err != nil {
				log.Println(err)
				return err
			}

			checkSum := checksum.CalculateCheckSum(data)

			// transfer data
			transferDataArgs := rpcChunkServer.TransferDataArgs{
				Data:     data,
				CheckSum: checkSum,
			}

			var transferDataReply rpcChunkServer.TransferDataReply

			err = conn.Call("ChunkServerAPI.TransferData", transferDataArgs, &transferDataReply)
			if err != nil {
				log.Println(err)
				return err
			}

			// Apply migration
			applyMigrationArgs := rpcChunkServer.ApplyMigrationArgs{
				ChunkID:  chunk.ID,
				CheckSum: checkSum,
				Offset:   0,
				Version:  chunk.Version,
			}

			var applyMigrationReply rpcChunkServer.ApplyMigrationReply

			err = conn.Call("ChunkServerAPI.ApplyMigration", applyMigrationArgs, &applyMigrationReply)
			if err != nil {
				log.Println(err)
				return err
			}

			return nil
		}(chunkServer)
	}

	wg.Wait()
	return nil
}
