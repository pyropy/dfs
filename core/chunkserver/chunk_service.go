package chunkserver

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/model"
	concurrentMap "github.com/pyropy/dfs/lib/concurrent_map"
	"log"
	"os"
	fp "path/filepath"
)

type ChunkService struct {
	Chunks concurrentMap.Map[uuid.UUID, model.Chunk]
}

var (
	ErrChunkDoesNotExist = errors.New("Chunk does not exist")
)

func NewChunkService() *ChunkService {
	return &ChunkService{
		Chunks: concurrentMap.NewMap[uuid.UUID, model.Chunk](),
	}
}

func GetChunkFilename(id uuid.UUID, index, version int) string {
	return fmt.Sprintf("%s-%d-%d.chunk", id, index, version)
}

func GetChunkPath(id uuid.UUID, filePath string, index, version int) string {
	filename := GetChunkFilename(id, index, version)
	filepath := fp.Join("chunks", filePath, filename)

	return filepath
}

func (cs *ChunkService) CreateChunk(id uuid.UUID, filePath string, index, version, size int) (*model.Chunk, error) {
	// create chunks parent path dir
	chunksParentPath := fp.Join("chunks", filePath)
	err := os.MkdirAll(chunksParentPath, 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	chunkPath := GetChunkPath(id, filePath, index, version)
	_, err = os.Create(chunkPath)
	if err != nil {
		return nil, err
	}

	chunk := model.Chunk{
		ID:       id,
		Version:  version,
		Path:     chunkPath,
		FilePath: filePath,
	}

	return &chunk, nil
}

func (cs *ChunkService) AddChunk(chunk model.Chunk) {
	cs.Chunks.Set(chunk.ID, chunk)
}

func (cs *ChunkService) GetChunk(chunkID uuid.UUID) (model.Chunk, bool) {
	existingChunk, exists := cs.Chunks.Get(chunkID)

	return *existingChunk, exists
}

func (cs *ChunkService) GetAllChunks() []model.Chunk {
	chunks := make([]model.Chunk, 0)

	cs.Chunks.Range(func(k, v any) bool {
		chunk := v.(model.Chunk)
		chunks = append(chunks, chunk)

		return true
	})

	return chunks
}

func (cs *ChunkService) ReadChunk(chunkID uuid.UUID, offset, length int) ([]byte, error) {
	return nil, nil
}

func (cs *ChunkService) WriteChunkBytes(chunkID uuid.UUID, data []byte, offset int, version int) (int, error) {
	chunk, exists := cs.GetChunk(chunkID)
	if !exists {
		return 0, ErrChunkDoesNotExist
	}

	if chunk.Version != version {
		log.Println("error", "chunkService", "chunk version missmatch", "chunkID", chunkID, "version", chunk.Version, "versionGiven", version)
		return 0, ErrChunkVersionMismatch
	}

	f, err := os.OpenFile(chunk.Path, os.O_RDWR, 0644)
	if err != nil {
		return 0, err
	}

	bytesWritten, err := f.WriteAt(data, int64(offset))
	if err != nil {
		return 0, err
	}

	return bytesWritten, nil
}

// IncrementChunkVersion increments chunk version number but also checks if
// there is a mismatch between version given by master and local chunk version
func (cs *ChunkServer) IncrementChunkVersion(chunkID uuid.UUID, version int) error {
	cs.Mutex.Lock()
	defer cs.Mutex.Unlock()

	chunk, exists := cs.Chunks.Get(chunkID)
	if !exists {
		return ErrChunkDoesNotExist
	}

	newPath := GetChunkPath(chunk.ID, chunk.FilePath, chunk.Index, version)
	err := os.Rename(chunk.Path, newPath)
	if err != nil {
		return err
	}

	currentVersion := chunk.Version
	if (currentVersion + 1) != version {
		return ErrChunkVersionMismatch
	}

	chunk.Path = newPath
	chunk.Version = chunk.Version + 1
	cs.Chunks.Set(chunk.ID, *chunk)

	return nil
}
