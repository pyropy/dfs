package master

import (
	"context"
	"github.com/pyropy/dfs/core/model"
	"time"
)

type GC struct {
	fileStore            *FileMetadataStore
	chunkMetaStore       *ChunkMetadataStore
	chunkServerMetaStore *ChunkServerMetadataStore
}

func NewGC(fileStore *FileMetadataStore, chunkMetaStore *ChunkMetadataStore, chunkServerMetaStore *ChunkServerMetadataStore) *GC {
	return &GC{
		fileStore:            fileStore,
		chunkMetaStore:       chunkMetaStore,
		chunkServerMetaStore: chunkServerMetaStore,
	}
}

// Start starts GC loop where all chunks are checked each 60 sec.
// If chunk is orphaned then it is sent to deletion channel.
func (gc *GC) Start(ctx context.Context) error {
	ticker := time.NewTicker(60 * time.Second)
	deletionChan := make(chan model.ChunkMetadata)

	log.Info("starting gc service")

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			go gc.findOrphanedChunks(deletionChan)
		case f := <-deletionChan:
			go gc.sweep(f)
		}
	}
}

// findOrphanedChunks goes through all chunks to and sends orphaned chunks to deletion channel
func (gc *GC) findOrphanedChunks(d chan model.ChunkMetadata) {
	gc.chunkMetaStore.Chunks.Range(func(k any, v any) bool {
		c := v.(model.ChunkMetadata)

		if hasParent := gc.fileStore.CheckFileExists(c.FilePath); !hasParent {
			d <- c
		}

		return true
	})
}

// sweep performs delete of a chunk
func (gc *GC) sweep(chunk model.ChunkMetadata) {
	var removedChunks int

	for _, csId := range chunk.ChunkServers {
		cs := gc.chunkServerMetaStore.GetChunkServerMetadata(csId)
		// TODO: Create some retry queue and or workerpool
		if err := deleteChunk(chunk.ID, cs); err != nil {
			log.Error("Error when deleting chunk", "chunkId", chunk.ID.String(), "chunkServerId", csId.String())
			continue
		}

		err := gc.chunkMetaStore.RemoveChunkHolderFromChunk(csId, chunk.ID)
		if err != nil {
			log.Error("Error when deleting chunk", "chunkId", chunk.ID.String(), "chunkServerId", csId.String())
		}
		removedChunks += 1
	}

	if removedChunks == len(chunk.ChunkServers) {
		log.Info("Cleaned chunk", "chunkId", chunk.ID, "chunkServers", chunk.ChunkServers)
		gc.chunkMetaStore.RemoveChunkMetadata(chunk.ID)
	}
}
