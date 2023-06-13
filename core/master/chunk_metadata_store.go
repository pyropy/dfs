package master

import (
	"errors"
	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/model"
	"github.com/pyropy/dfs/lib/cmap"
	"github.com/pyropy/dfs/lib/utils"
)

var (
	ErrChunkNotFound = errors.New("chunk not found")
)

type ChunkMetadataStore struct {
	Chunks cmap.Map[uuid.UUID, model.ChunkMetadata]
}

func NewChunkMetadataStore() *ChunkMetadataStore {
	return &ChunkMetadataStore{
		Chunks: cmap.NewMap[uuid.UUID, model.ChunkMetadata](),
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

func (cs *ChunkMetadataStore) AddNewChunkMetadata(chunk model.ChunkMetadata) {
	cs.Chunks.Set(chunk.ID, chunk)
}

func (cs *ChunkMetadataStore) GetChunkHolders(chunkID uuid.UUID) []uuid.UUID {
	chunk, chunkExists := cs.Chunks.Get(chunkID)

	if !chunkExists {
		return []uuid.UUID{}
	}

	return chunk.ChunkServers
}

func (cs *ChunkMetadataStore) GetChunk(chunkID uuid.UUID) (*model.ChunkMetadata, error) {

	chunk, chunkExists := cs.Chunks.Get(chunkID)
	if !chunkExists {
		return nil, ErrChunkNotFound
	}

	return chunk, nil
}

func (cs *ChunkMetadataStore) IncrementChunkVersion(chunkID uuid.UUID) (int, error) {
	chunk, chunkExists := cs.Chunks.Get(chunkID)

	if !chunkExists {
		return 0, ErrChunkNotFound
	}

	chunk.Version++
	cs.Chunks.Set(chunkID, *chunk)

	return chunk.Version, nil
}

// UpdateChunksLocation updates chunk location on chunk server heart beat reported to master
func (cs *ChunkMetadataStore) UpdateChunksLocation(chunkHolder uuid.UUID, chunks []model.ChunkMetadata) {
	chunkIds := []uuid.UUID{}
	for _, c := range chunks {
		chunkIds = append(chunkIds, c.ID)
	}

	cs.Chunks.Range(func(k, v any) bool {
		chunkID := k.(uuid.UUID)
		chunk := v.(model.ChunkMetadata)
		inChunkHolders := utils.Contains(chunk.ChunkServers, chunkHolder)
		isCurrentlyHoldingChunk := utils.Contains(chunkIds, chunkID)

		switch {
		case inChunkHolders && !isCurrentlyHoldingChunk:
			chunk.ChunkServers = utils.Remove(chunk.ChunkServers, chunkHolder)
			log.Info("Removed")
		case !inChunkHolders && isCurrentlyHoldingChunk:
			chunk.ChunkServers = append(chunk.ChunkServers, chunkHolder)
			log.Info("Appended")
		default:
			log.Info("Nothing happened")
		}

		cs.Chunks.Set(chunkID, chunk)

		return true
	})
}

// RemoveChunkHolder removes given chunk holder from list of chunk holders for all chunks
func (cs *ChunkMetadataStore) RemoveChunkHolder(chunkHolderID uuid.UUID) {
	cs.Chunks.Range(func(k, v any) bool {
		chunkMetadata := v.(model.ChunkMetadata)
		chunkServers := make([]uuid.UUID, 0)

		for _, chunkServerID := range chunkMetadata.ChunkServers {
			if chunkHolderID != chunkServerID {
				chunkServers = append(chunkServers, chunkServerID)
			}
		}

		chunkMetadata.ChunkServers = chunkServers
		cs.Chunks.Set(chunkMetadata.ID, chunkMetadata)
		return true
	})
}
