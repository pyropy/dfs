package master

import (
	"errors"
	"github.com/pyropy/dfs/core/chunk_metadata_service"
	"github.com/pyropy/dfs/core/constants"
	fileMeta "github.com/pyropy/dfs/core/file_metadata_service"
	"github.com/pyropy/dfs/core/model"
	chunkServerRPC "github.com/pyropy/dfs/rpc/chunkserver"
	"log"
	"math/rand"
	"net/rpc"
	"time"

	"github.com/google/uuid"
)

type Master struct {
	*LeaseService
	*fileMeta.FileMetadataService
	*chunkmetadataservice.ChunkMetadataService
	*ChunkServerMetadataService
}

var (
	ErrFileExists          = errors.New("file exists")
	ErrFileCreation        = errors.New("failed to create file")
	ErrChunkHolderNotFound = errors.New("chunk holder not found")
)

func NewMaster() *Master {
	return &Master{
		LeaseService:               NewLeaseService(),
		FileMetadataService:        fileMeta.NewFileMetadataService(),
		ChunkMetadataService:       chunkmetadataservice.NewChunkMetadataService(),
		ChunkServerMetadataService: NewChunkServerMetadataService(),
	}
}

// CreateNewFile selects chunk servers and instructs them to create N number of chunks with predefined IDs
func (m *Master) CreateNewFile(filePath string, fileSizeBytes, repFactor, chunkSizeBytes int) (*fileMeta.FileMetadata, []uuid.UUID, error) {
	// TODO: Add file namespace locks
	var chunkIds []uuid.UUID
	var chunkMetadata []model.ChunkMetadata
	var chunkServerIds []uuid.UUID

	fileExists := m.FileMetadataService.CheckFileExists(filePath)
	if fileExists {
		return nil, chunkIds, ErrFileExists
	}

	chunkVersion := constants.INITIAL_CHUNK_VERSION
	chunkServers := m.ChunkServerMetadataService.SelectChunkServers(repFactor)
	fileMetadata := fileMeta.NewFileMetadata(filePath)
	numChunks := (fileSizeBytes + (chunkSizeBytes - 1)) / chunkSizeBytes

	for _, cs := range chunkServers {
		chunkServerIds = append(chunkServerIds, cs.ID)
	}

	for i := 0; i < numChunks; i++ {
		chunkID := uuid.New()
		chunkIds = append(chunkIds, chunkID)
		fileMetadata.Chunks = append(fileMetadata.Chunks, chunkID)
		chunk := chunkmetadataservice.NewChunkMetadata(chunkID, i, chunkVersion, chunkServerIds)
		chunkMetadata = append(chunkMetadata, chunk)

		for _, chunkServer := range chunkServers {
			err := m.createNewChunk(chunkID, filePath, chunkSizeBytes, chunkVersion, &chunkServer)
			if err != nil {
				return nil, nil, ErrFileCreation
			}
		}
	}

	// Add file metadata
	m.FileMetadataService.AddNewFileMetadata(filePath, fileMetadata)

	// Add chunk metadata for each chunk created
	for _, chunkMetadata := range chunkMetadata {
		m.ChunkMetadataService.AddNewChunkMetadata(chunkMetadata)
	}

	return &fileMetadata, chunkServerIds, nil
}

func (m *Master) RequestWrite(chunkID uuid.UUID) (*model.Lease, int, error) {
	chunkServers := m.GetChunkHolders(chunkID)
	if len(chunkServers) == 0 {
		return nil, 0, ErrChunkHolderNotFound
	}

	chunkVersion, err := m.IncrementChunkVersion(chunkID)
	if err != nil {
		return nil, 0, err
	}

	randomIndex := rand.Intn(len(chunkServers))
	chunkServerID := chunkServers[randomIndex]
	chunkServerMetadata := m.GetChunkServerMetadata(chunkServerID)
	lease := m.LeaseService.GrantLease(chunkID, chunkServerMetadata)

	err = m.sendLeaseGrant(chunkID, lease, chunkServerMetadata)
	if err != nil {
		return nil, 0, err
	}

	for _, chunkServerID := range chunkServers {
		chunkServerMetadata := m.GetChunkServerMetadata(chunkServerID)
		// TODO: Retry if fails
		err := m.incrementChunkVersion(chunkID, chunkVersion, chunkServerMetadata)

		if err != nil {
			return nil, 0, err
		}
	}

	return lease, chunkVersion, nil
}

func (m *Master) RequestLeaseRenewal(chunkID uuid.UUID, chunkServer *ChunkServerMetadata) (*model.Lease, error) {
	return m.ExtendLease(chunkID, chunkServer)
}

func (m *Master) StartHealthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	unhealthyChan := make(chan uuid.UUID)

	for {
		select {
		case _ = <-ticker.C:
			for _, cs := range m.ChunkServers {
				if !cs.Active {
					continue
				}

				go HealthCheck(cs, unhealthyChan)
			}
		case unhealthyChunkServerID := <-unhealthyChan:
			m.ChunkServerMetadataService.MarkUnhealthy(unhealthyChunkServerID)
		}
	}
}

func (m *Master) createNewChunk(id uuid.UUID, filePath string, size int, chunkVersion int, chunkServer *ChunkServerMetadata) error {
	args := chunkServerRPC.CreateChunkRequest{
		ChunkID:      id,
		ChunkSize:    size,
		ChunkVersion: chunkVersion,
		FilePath:     filePath,
	}

	reply := chunkServerRPC.CreateChunkReply{}
	return callChunkServerRPC(chunkServer, "ChunkServerAPI.CreateChunk", args, &reply)
}

func (m *Master) sendLeaseGrant(chunkID uuid.UUID, lease *model.Lease, chunkServer *ChunkServerMetadata) error {
	args := chunkServerRPC.GrantLeaseArgs{
		ChunkID:    chunkID,
		ValidUntil: lease.ValidUntil,
	}

	reply := chunkServerRPC.GrantLeaseReply{}
	return callChunkServerRPC(chunkServer, "ChunkServerAPI.GrantLease", args, &reply)
}

func (m *Master) incrementChunkVersion(chunkID uuid.UUID, version int, chunkServer *ChunkServerMetadata) error {
	args := chunkServerRPC.IncrementChunkVersionArgs{
		ChunkID: chunkID,
		Version: version,
	}
	reply := chunkServerRPC.IncrementChunkVersionReply{}

	return callChunkServerRPC(chunkServer, "ChunkServerAPI.IncrementChunkVersion", args, &reply)
}

func callChunkServerRPC(chunkServer *ChunkServerMetadata, method string, args interface{}, reply interface{}) error {
	client, err := rpc.DialHTTP("tcp", chunkServer.Address)
	if err != nil {
		log.Println("error", chunkServer.Address, "unreachable")
		return err
	}

	defer client.Close()

	err = client.Call(method, args, reply)
	if err != nil {
		log.Println("error", chunkServer.Address, "error", err)
		return err
	}

	return nil
}
