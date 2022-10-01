package core

import (
	"errors"
	"github.com/google/uuid"
	chunkServerRPC "github.com/pyropy/dfs/business/rpc/chunkserver"
	"log"
	"net/rpc"
	"sync"
	"time"
)

type ChunkServerMetadata struct {
	ID      uuid.UUID
	Address string
	Healthy bool
}

type ChunkID = uuid.UUID

type ChunkMetadata struct {
	ChunkID      uuid.UUID
	Version      int
	ChunkServers []uuid.UUID
	Lease        uuid.UUID
	Checksum     int
}

type FileMetadata struct {
	ID     uuid.UUID
	Path   string
	Chunks []ChunkID
}

type FilePath = string

type Master struct {
	Mutex        sync.Mutex
	Chunkservers []ChunkServerMetadata
	Files        map[FilePath]FileMetadata
	Chunks       map[ChunkID]ChunkMetadata
}

var (
	ErrFileCreation = errors.New("Failed to create file.")
)

func NewMaster() *Master {
	return &Master{
		Chunkservers: []ChunkServerMetadata{},
		Chunks:       map[ChunkID]ChunkMetadata{},
		Files:        map[FilePath]FileMetadata{},
	}
}

func (m *Master) RegisterNewChunkServer(addr string) *ChunkServerMetadata {
	chunkServerMetadata := ChunkServerMetadata{ID: uuid.New(), Address: addr, Healthy: true}
	m.Chunkservers = append(m.Chunkservers, chunkServerMetadata)
	return &chunkServerMetadata
}

func (m *Master) StartHealthCheckService() {
	ticker := time.NewTicker(30 * time.Second)

	checkHealth := func(i int) {
		chunkServer := m.Chunkservers[i]
		client, err := rpc.DialHTTP("tcp", chunkServer.Address)
		if err != nil {
			log.Println("error", chunkServer.Address, "unreachable")
			m.Chunkservers[i].Healthy = false
			return
		}

		var reply chunkServerRPC.HealthCheckReply
		args := &chunkServerRPC.HealthCheckArgs{}
		err = client.Call("ChunkServerAPI.HealthCheck", args, &reply)
		if err != nil {
			log.Println("error", chunkServer.Address, "error", err)
			m.Chunkservers[i].Healthy = false
			return
		}

		if reply.Status < 299 {
			m.Chunkservers[i].Healthy = true
			return
		}

		m.Chunkservers[i].Healthy = false
	}

	for {
		select {
		case _ = <-ticker.C:
			for i := 0; i < len(m.Chunkservers); i++ {
				go checkHealth(i)
			}
		}
	}
}

// CreateNewFile selects chunk servers and instructs them to create N number of chunks with predefined IDs
func (m *Master) CreateNewFile(filePath string, fileSizeBytes, repFactor, chunkSizeBytes int) (*FileMetadata, error) {
	var chunkIds []ChunkID
	var chunkServerIds []uuid.UUID

	chunkServers := m.SelectChunkServers(repFactor)
	numChunks := fileSizeBytes / chunkSizeBytes

	for _, cs := range chunkServers {
		chunkServerIds = append(chunkServerIds, cs.ID)
		for i := 0; i < numChunks; i++ {
			chunkID := uuid.New()
			err := m.createNewChunk(chunkID, chunkSizeBytes, cs)
			if err != nil {
				return nil, ErrFileCreation
			}

			chunkIds = append(chunkIds, chunkID)
		}
	}

	fileMetadata := FileMetadata{
		ID:     uuid.New(),
		Path:   filePath,
		Chunks: chunkIds,
	}

	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	// Add file metadata
	m.Files[filePath] = fileMetadata

	// Add chunk metadata for each chunk created
	for _, chunkId := range chunkIds {
		m.Chunks[chunkId] = ChunkMetadata{
			ChunkID:      chunkId,
			Version:      1,
			ChunkServers: chunkServerIds,
		}
	}

	return &fileMetadata, nil
}

func (m *Master) createNewChunk(id uuid.UUID, size int, chunkServer ChunkServerMetadata) error {
	client, err := rpc.DialHTTP("tcp", chunkServer.Address)
	if err != nil {
		log.Println("error", chunkServer.Address, "unreachable")
		return err
	}

	args := chunkServerRPC.CreateChunkRequest{
		ChunkID:   id,
		ChunkSize: size,
	}

	reply := chunkServerRPC.CreateChunkReply{}
	err = client.Call("ChunkServerAPI.CreateChunk", args, &reply)
	if err != nil {
		log.Println("error", chunkServer.Address, "error", err)
		return err
	}

	return nil
}

func (m *Master) SelectChunkServers(num int) []ChunkServerMetadata {
	return m.Chunkservers[:num-1]
}
