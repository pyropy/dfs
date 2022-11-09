package master

import (
	"errors"
	"log"
	"math/rand"
	"net/rpc"

	"github.com/google/uuid"
	chunkmeta "github.com/pyropy/dfs/business/core/chunk_metadata_service"
	filemeta "github.com/pyropy/dfs/business/core/file_metadata_service"
	chunkServerRPC "github.com/pyropy/dfs/business/rpc/chunkserver"
)

type Master struct {
	*LeaseService
	*filemeta.FileMetadataService
	*chunkmeta.ChunkMetadataService
	*ChunkServerMetadataService
}

var (
	ErrFileExists          = errors.New("File exists")
	ErrFileCreation        = errors.New("Failed to create file.")
	ErrChunkHolderNotFound = errors.New("Chunk holder not found.")
)

func NewMaster() *Master {
	return &Master{
		LeaseService:               NewLeaseService(),
		FileMetadataService:        filemeta.NewFileMetadataService(),
		ChunkMetadataService:       chunkmeta.NewChunkMetadataService(),
		ChunkServerMetadataService: NewChunkServerMetadataService(),
	}
}

// TODO: Add file namespace locks
// CreateNewFile selects chunk servers and instructs them to create N number of chunks with predefined IDs
func (m *Master) CreateNewFile(filePath string, fileSizeBytes, repFactor, chunkSizeBytes int) (*filemeta.FileMetadata, []uuid.UUID, error) {
	var chunkIds []uuid.UUID
	var chunkMetadata []chunkmeta.ChunkMetadata
	var chunkServerIds []uuid.UUID

	fileExists := m.FileMetadataService.CheckFileExists(filePath)
	if fileExists {
		return nil, chunkIds, ErrFileExists
	}

	chunkVersion := 1
	chunkServers := m.ChunkServerMetadataService.SelectChunkServers(repFactor)
	fileMetadata := filemeta.NewFileMetadata(filePath)
	numChunks := (fileSizeBytes + (chunkSizeBytes - 1)) / chunkSizeBytes

	for _, cs := range chunkServers {
		chunkServerIds = append(chunkServerIds, cs.ID)
	}

	for i := 0; i < numChunks; i++ {
		chunkID := uuid.New()
		chunkIds = append(chunkIds, chunkID)
		fileMetadata.Chunks = append(fileMetadata.Chunks, chunkID)
		chunk := chunkmeta.NewChunkMetadata(chunkID, i, chunkVersion, chunkServerIds)
		chunkMetadata = append(chunkMetadata, chunk)

		for _, chunkServer := range chunkServers {
			err := m.createNewChunk(chunkID, chunkSizeBytes, chunkVersion, &chunkServer)
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

func (m *Master) RequestWrite(chunkID uuid.UUID) (*Lease, error) {
	chunkServers := m.GetChunkHolders(chunkID)
	if len(chunkServers) == 0 {
		return nil, ErrChunkHolderNotFound
	}

	chunkVersion, err := m.IncrementChunkVersion(chunkID)
	if err != nil {
		return nil, err
	}

	randomIndex := rand.Intn(len(chunkServers))
	chunkServerID := chunkServers[randomIndex]
	chunkServerMetadata := m.GetChunkServerMetadata(chunkServerID)
	lease := m.LeaseService.GrantLease(chunkID, chunkServerMetadata)

	err = m.sendLeaseGrant(chunkID, lease, chunkServerMetadata)
	if err != nil {
		return nil, err
	}

	err = m.incrementChunkVersion(chunkID, chunkVersion, chunkServerMetadata)
	if err != nil {
		return nil, err
	}

	return lease, nil
}

func (m *Master) RequestLeaseRenewal(chunkID uuid.UUID, chunkServer *ChunkServerMetadata) (*Lease, error) {
	return m.ExtendLease(chunkID, chunkServer)
}

func (m *Master) createNewChunk(id uuid.UUID, size int, chunkVersion int, chunkServer *ChunkServerMetadata) error {
	args := chunkServerRPC.CreateChunkRequest{
		ChunkID:      id,
		ChunkSize:    size,
		ChunkVersion: chunkVersion,
	}

	reply := chunkServerRPC.CreateChunkReply{}
	return callChunkServerRPC(chunkServer, "ChunkServerAPI.CreateChunk", args, &reply)
}

func (m *Master) sendLeaseGrant(chunkID uuid.UUID, lease *Lease, chunkServer *ChunkServerMetadata) error {
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
