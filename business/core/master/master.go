package master

import (
	"errors"
	"github.com/google/uuid"
	chunkServerRPC "github.com/pyropy/dfs/business/rpc/chunkserver"
	"log"
	"net/rpc"
)

type Master struct {
	*FileMetadataService
	*ChunkMetadataService
	*ChunkServerMetadataService
}

var (
	ErrFileExists   = errors.New("File exists")
	ErrFileCreation = errors.New("Failed to create file.")
)

func NewMaster() *Master {
	return &Master{
		FileMetadataService:        NewFileMetadataService(),
		ChunkMetadataService:       NewChunkMetadataService(),
		ChunkServerMetadataService: NewChunkServerMetadataService(),
	}
}

// TODO: Add file namespace locks
// CreateNewFile selects chunk servers and instructs them to create N number of chunks with predefined IDs
func (m *Master) CreateNewFile(filePath string, fileSizeBytes, repFactor, chunkSizeBytes int) (*FileMetadata, []uuid.UUID, error) {
	var chunkIds []ChunkID
	var chunkMetadata []ChunkMetadata
	var chunkServerIds []uuid.UUID

	fileExists := m.FileMetadataService.CheckFileExists(filePath)
	if fileExists {
		return nil, chunkIds, ErrFileExists
	}

	chunkServers := m.ChunkServerMetadataService.SelectChunkServers(repFactor)
	fileMetadata := NewFileMetadata(filePath)
	numChunks := (fileSizeBytes + (chunkSizeBytes - 1)) / chunkSizeBytes

	for _, cs := range chunkServers {
		chunkServerIds = append(chunkServerIds, cs.ID)
	}

	for i := 0; i < numChunks; i++ {
		chunkID := uuid.New()
		chunkIds = append(chunkIds, chunkID)
		fileMetadata.Chunks = append(fileMetadata.Chunks, chunkID)
		chunk := NewChunkMetadata(chunkID, 1, chunkServerIds)
		chunkMetadata = append(chunkMetadata, chunk)

		for _, chunkServer := range chunkServers {
			err := m.createNewChunk(chunkID, chunkSizeBytes, chunkServer)
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
