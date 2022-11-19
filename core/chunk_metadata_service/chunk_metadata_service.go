package chunkmetadataservice

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

var (
	ErrChunkNotFound = errors.New("Chunk not found.")
)

type ChunkMetadataService struct {
	Mutex  sync.RWMutex
	Chunks map[uuid.UUID]ChunkMetadata
}

func NewChunkMetadataService() *ChunkMetadataService {
	return &ChunkMetadataService{
		Chunks: map[uuid.UUID]ChunkMetadata{},
	}
}

func NewChunkMetadata(chunkID uuid.UUID, index, version int, chunkServerIds []uuid.UUID) ChunkMetadata {
	return ChunkMetadata{
		ChunkID:      chunkID,
		Index:        index,
		Version:      version,
		ChunkServers: chunkServerIds,
	}
}

func (cs *ChunkMetadataService) AddNewChunkMetadata(chunk ChunkMetadata) {
	cs.Mutex.Lock()
	defer cs.Mutex.Unlock()

	cs.Chunks[chunk.ChunkID] = chunk
}

func (cs *ChunkMetadataService) GetChunkHolders(chunkID uuid.UUID) []uuid.UUID {
	cs.Mutex.RLock()
	defer cs.Mutex.RUnlock()

	chunk, chunkExists := cs.Chunks[chunkID]

	if !chunkExists {
		return []uuid.UUID{}
	}

	return chunk.ChunkServers
}

func (cs *ChunkMetadataService) GetChunk(chunkID uuid.UUID) (*ChunkMetadata, error) {
	cs.Mutex.RLock()
	defer cs.Mutex.RUnlock()

	chunk, chunkExists := cs.Chunks[chunkID]
	if !chunkExists {
		return nil, ErrChunkNotFound
	}

	return &chunk, nil
}

func (cs *ChunkMetadataService) IncrementChunkVersion(chunkID uuid.UUID) (int, error) {
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
