package chunkserver

import (
	"github.com/google/uuid"
	"sync"
)

type Chunk struct {
	ID       uuid.UUID
	Version  int
	Path     string // disk path
	Checksum int
}

type ChunkService struct {
	Mutex  sync.RWMutex
	Chunks map[uuid.UUID]Chunk
}

func NewChunkService() *ChunkService {
	return &ChunkService{
		Chunks: map[uuid.UUID]Chunk{},
	}
}

func (cs *ChunkService) AddChunk(chunk Chunk) {
	cs.Mutex.Lock()
	defer cs.Mutex.Unlock()

	cs.Chunks[chunk.ID] = chunk
}

func (cs *ChunkService) GetChunk(chunkID uuid.UUID) (Chunk, bool) {
	cs.Mutex.RLock()
	defer cs.Mutex.RUnlock()

	existingChunk, exists := cs.Chunks[chunkID]

	return existingChunk, exists
}

// IncrementChunkVersion increments chunk version number but also checks if
// there is a mismatch between version given by master and local chunk version
func (cs *ChunkServer) IncrementChunkVersion(chunkID uuid.UUID, version int) error {
	cs.Mutex.Lock()
	defer cs.Mutex.Unlock()

	chunk, exists := cs.Chunks[chunkID]
	if !exists {
		return ErrChunkDoesNotExist
	}

	currentVersion := chunk.Version

	if (currentVersion + 1) != version {
		return ErrChunkVersionMismatch
	}

	chunk.Version = chunk.Version + 1
	cs.Chunks[chunk.ID] = chunk

	return nil
}
