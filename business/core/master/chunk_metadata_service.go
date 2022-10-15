package master

import (
	"sync"

	"github.com/google/uuid"
)

type ChunkID = uuid.UUID

type ChunkMetadata struct {
	ChunkID      uuid.UUID
	Version      int
	ChunkServers []uuid.UUID
	Lease        uuid.UUID
	Checksum     int
}

type ChunkMetadataService struct {
	Mutex  sync.RWMutex
	Chunks map[ChunkID]ChunkMetadata
}

func NewChunkMetadataService() *ChunkMetadataService {
	return &ChunkMetadataService{
		Chunks: map[ChunkID]ChunkMetadata{},
	}
}

func NewChunkMetadata(chunkID uuid.UUID, version int, chunkServerIds []uuid.UUID) ChunkMetadata {
	return ChunkMetadata{
		ChunkID:      chunkID,
		Version:      1,
		ChunkServers: chunkServerIds,
	}
}

func (cs *ChunkMetadataService) AddNewChunkMetadata(chunk ChunkMetadata) {
	cs.Mutex.Lock()
	defer cs.Mutex.Unlock()

	cs.Chunks[chunk.ChunkID] = chunk
}
