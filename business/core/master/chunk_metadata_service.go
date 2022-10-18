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

func (cs *ChunkMetadataService) GetChunkHolders(chunkID ChunkID) []uuid.UUID {
	cs.Mutex.RLock()
	defer cs.Mutex.RUnlock()

	chunk, chunkExists := cs.Chunks[chunkID]

	if !chunkExists {
		return []uuid.UUID{}
	}

	return chunk.ChunkServers
}

func (cs *ChunkMetadataService) GetChunk(chunkID ChunkID) (*ChunkMetadata, error) {
	cs.Mutex.RLock()
	defer cs.Mutex.RUnlock()

	chunk, chunkExists := cs.Chunks[chunkID]
	if !chunkExists {
		return nil, ErrChunkNotFound
	}

	return &chunk, nil
}

func (cs *ChunkMetadataService) IncrementChunkVersion(chunkID ChunkID) (int, error) {
	cs.Mutex.Lock()
	defer cs.Mutex.Unlock()

	chunk, chunkExists := cs.Chunks[chunkID]
	if !chunkExists {
		return 0, ErrChunkNotFound
	}

	chunk.Version++
	cs.Chunks[chunkID] = chunk

	return chunk.Version, nil
}
