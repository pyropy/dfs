package chunkmetadataservice

import (
	"errors"
	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/model"
	concurrentMap "github.com/pyropy/dfs/lib/concurrent_map"
)

var (
	ErrChunkNotFound = errors.New("Chunk not found.")
)

type ChunkMetadataService struct {
	Chunks concurrentMap.Map[uuid.UUID, model.ChunkMetadata]
}

func NewChunkMetadataService() *ChunkMetadataService {
	return &ChunkMetadataService{
		Chunks: concurrentMap.NewMap[uuid.UUID, model.ChunkMetadata](),
	}
}

func NewChunkMetadata(chunkID uuid.UUID, index, version int, chunkServerIds []uuid.UUID) model.ChunkMetadata {
	return model.ChunkMetadata{
		Chunk: model.Chunk{
			ID:      chunkID,
			Index:   index,
			Version: version,
		},
		ChunkServers: chunkServerIds,
	}
}

func (cs *ChunkMetadataService) AddNewChunkMetadata(chunk model.ChunkMetadata) {
	cs.Chunks.Set(chunk.ID, chunk)
}

func (cs *ChunkMetadataService) GetChunkHolders(chunkID uuid.UUID) []uuid.UUID {
	chunk, chunkExists := cs.Chunks.Get(chunkID)

	if !chunkExists {
		return []uuid.UUID{}
	}

	return chunk.ChunkServers
}

func (cs *ChunkMetadataService) GetChunk(chunkID uuid.UUID) (*model.ChunkMetadata, error) {

	chunk, chunkExists := cs.Chunks.Get(chunkID)
	if !chunkExists {
		return nil, ErrChunkNotFound
	}

	return chunk, nil
}

func (cs *ChunkMetadataService) IncrementChunkVersion(chunkID uuid.UUID) (int, error) {

	chunk, chunkExists := cs.Chunks.Get(chunkID)
	if !chunkExists {
		return 0, ErrChunkNotFound
	}

	chunk.Version++
	cs.Chunks.Set(chunkID, *chunk)

	return chunk.Version, nil
}

// UpdateChunksLocation updates chunk location on chunk server heart beat reported to master
func (cs *ChunkMetadataService) UpdateChunksLocation(chunkHolder uuid.UUID, chunks []model.ChunkMetadata) {
	for _, c := range chunks {
		chunk, chunkExists := cs.Chunks.Get(c.ID)
		if !chunkExists {
			cp := c
			chunk = &cp
		}

		chunk.ChunkServers = append(chunk.ChunkServers, chunkHolder)
		cs.Chunks.Set(chunk.ID, *chunk)
	}
}
