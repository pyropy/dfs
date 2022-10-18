package chunkserver

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	fp "path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pyropy/dfs/business/rpc/master"
)

// TODO: Implement some kinda Tree Structure
// to hold file/dir metadata so users can traverse filesystem
type ChunkServer struct {
	*ChunkService
	*LeaseService

	Mutex         sync.RWMutex
	MasterAddr    string
	ChunkServerID uuid.UUID
	LeaseExpChan  chan *Lease
}

var (
	ErrChunkDoesNotExist    = errors.New("Chunk does not exist")
	ErrChunkAlreadyExists   = errors.New("Chunk already exists")
	ErrChunkVersionMismatch = errors.New("Chunk version mismatch")
)

func NewChunkServer() *ChunkServer {
	leaseExpChan := make(chan *Lease)

	return &ChunkServer{
		LeaseExpChan: leaseExpChan,
		ChunkService: NewChunkService(),
		LeaseService: NewLeaseService(leaseExpChan),
	}
}

func (c *ChunkServer) CreateChunk(id uuid.UUID, version, sizeBytes int) (*Chunk, error) {
	existingChunk, exists := c.GetChunk(id)
	if exists && existingChunk.Version == version {
		return nil, ErrChunkAlreadyExists
	}

	filename := fmt.Sprintf("%s-%d", id, version)
	filepath := fp.Join("chunks", filename)
	_, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}

	err = os.Truncate(filepath, int64(sizeBytes))
	if err != nil {
		return nil, err
	}

	chunk := Chunk{
		ID:      id,
		Version: version,
		Path:    filepath,
	}

	c.AddChunk(chunk)

	return &chunk, nil
}

func (c *ChunkServer) GrantLease(chunkID uuid.UUID, validUntil time.Time) error {
	_, exists := c.GetChunk(chunkID)
	if !exists {
		return ErrChunkDoesNotExist
	}

	c.LeaseService.GrantLease(chunkID, validUntil)

	return nil

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

	c.ChunkServerID = c.ChunkServerID

	return nil
}

func (c *ChunkServer) MonitorExpiredLeases() {
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

	err = client.Call("MasterAPI.RequestLeaseRenewalReply ", args, &reply)
	if err != nil {
		return err
	}

	return nil

}
