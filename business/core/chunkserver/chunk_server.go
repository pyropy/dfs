package chunkserver

import (
	"errors"
	"log"
	"net/rpc"
	"sync"
	"time"

	"github.com/google/uuid"
	lru "github.com/pyropy/dfs/business/core/lru_cache"
	rpcChunkServer "github.com/pyropy/dfs/business/rpc/chunkserver"
	"github.com/pyropy/dfs/business/rpc/master"
)

// TODO: Implement some kinda Tree Structure
// to hold file/dir metadata so users can traverse filesystem
type ChunkServer struct {
	*ChunkService
	*LeaseService

	Mutex         sync.RWMutex
	LRU           *lru.LRU
	MasterAddr    string
	ChunkServerID uuid.UUID
	LeaseExpChan  chan *Lease
}

var (
	ErrChunkAlreadyExists   = errors.New("Chunk already exists")
	ErrChunkVersionMismatch = errors.New("Chunk version mismatch")
	ErrChunkLeaseNotGranted = errors.New("Chunk lease not granted")
	ErrDataNotFoundInCache  = errors.New("Data not found in cache.")
)

func NewChunkServer() *ChunkServer {
	leaseExpChan := make(chan *Lease)

	return &ChunkServer{
		LeaseExpChan: leaseExpChan,
		LRU:          lru.NewLRU(100),
		ChunkService: NewChunkService(),
		LeaseService: NewLeaseService(leaseExpChan),
	}
}

func (c *ChunkServer) CreateChunk(id uuid.UUID, index, version, sizeBytes int) (*Chunk, error) {
	existingChunk, exists := c.GetChunk(id)
	if exists && existingChunk.Version == version {
		return nil, ErrChunkAlreadyExists
	}

	chunk, err := c.ChunkService.CreateChunk(id, index, version)
	if err != nil {
		return nil, err
	}

	c.AddChunk(*chunk)

	return chunk, nil
}

func (c *ChunkServer) WriteChunk(chunkID uuid.UUID, checksum int, offset int, version int, chunkHolders []rpcChunkServer.ChunkServer) (int, error) {
	data, exists := c.LRU.Get(checksum)
	if !exists {
		return 0, ErrDataNotFoundInCache
	}

	bytesWritten, err := c.WriteChunkBytes(chunkID, data, offset, version)
	if err != nil {
		return bytesWritten, err
	}

	// If primary notify other holders to apply migration
	if !c.HaveLease(chunkID) {
		return bytesWritten, nil
	}

	for _, ch := range chunkHolders {
		if ch.ID == c.ChunkServerID {
			continue
		}

		err := c.SendApplyMigration(chunkID, checksum, offset, version, ch.Address)
		if err != nil {
			log.Println("error", "chunkServer", "failed to send apply migration", err)
		}

	}

	return bytesWritten, nil
}

func (c *ChunkServer) GrantLease(chunkID uuid.UUID, validUntil time.Time) error {
	_, exists := c.GetChunk(chunkID)
	if !exists {
		return ErrChunkDoesNotExist
	}

	c.LeaseService.GrantLease(chunkID, validUntil)

	return nil
}

func (c *ChunkServer) RecieveBytes(data []byte, checksum int) error {
	// TODO: Check checksum
	c.LRU.Put(checksum, data)

	return nil
}

func (c *ChunkServer) MonitorExpiredLeases() {
	go c.MonitorLeases()

	for {
		select {
		case lease := <-c.leaseExpChan:
			err := c.RequestLeaseRenewal(lease)
			if err != nil {
				log.Println("error", "chunkServer", "lease renewal failed", lease, err)
			}
		default:
		}
	}
}

// RegisterChunkServer registers chunk server instance with Master API
func (c *ChunkServer) RegisterChunkServer(masterAddr, addr string) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	client, err := rpc.DialHTTP("tcp", masterAddr)
	if err != nil {
		log.Println("error", "unreachable")
		return err
	}

	c.MasterAddr = masterAddr

	var reply master.RegisterReply
	args := &master.RegisterArgs{Address: addr}
	err = client.Call("MasterAPI.RegisterChunkServer", args, &reply)
	if err != nil {
		return err
	}

	c.ChunkServerID = reply.ID

	return nil
}

// RequestLeaseRenewal requests renewal for given lease from master
func (c *ChunkServer) RequestLeaseRenewal(lease *Lease) error {
	client, err := rpc.DialHTTP("tcp", c.MasterAddr)
	if err != nil {
		log.Println("error", "unreachable")
		return err
	}

	var reply master.RequestLeaseRenewalReply
	args := &master.RequestLeaseRenewalArgs{
		ChunkID:       lease.ChunkID,
		ChunkServerID: c.ChunkServerID,
	}

	err = client.Call("MasterAPI.RequestLeaseRenewal", args, &reply)
	if err != nil {
		return err
	}

	if !reply.Granted {
		return ErrChunkLeaseNotGranted
	}

	c.LeaseService.GrantLease(reply.ChunkID, reply.ValidUntil)

	log.Println("info", "chunkServer", "lease granted", reply.ChunkID, reply.ValidUntil)
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
