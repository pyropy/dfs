package chunkserver

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/model"
	"github.com/pyropy/dfs/lib/cmap"
	"log"
	"os"
	fp "path/filepath"
)

type ChunkService struct {
	Cfg    *Config
	Chunks cmap.Map[uuid.UUID, model.Chunk]
}

var (
	ErrChunkDoesNotExist = errors.New("Chunk does not exist")
)

func NewChunkService(cfg *Config) *ChunkService {
	return &ChunkService{
		Cfg:    cfg,
		Chunks: cmap.NewMap[uuid.UUID, model.Chunk](),
	}
}

func GetChunkFilename(id uuid.UUID, index, version int) string {
	return fmt.Sprintf("%s-%d-%d.chunk", id, index, version)
}

func (cs *ChunkService) GetChunkPath(id uuid.UUID, filePath string, index, version int) string {
	filename := GetChunkFilename(id, index, version)
	filepath := fp.Join(cs.Cfg.Chunks.Path, filePath, filename)

	return filepath
}

func (cs *ChunkService) CreateChunk(id uuid.UUID, filePath string, index, version, size int) (*model.Chunk, error) {
	// create chunks parent path dir
	chunksParentPath := fp.Join(cs.Cfg.Chunks.Path, filePath)
	err := os.MkdirAll(chunksParentPath, 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	chunkPath := cs.GetChunkPath(id, filePath, index, version)
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

func (cs *ChunkService) GetChunk(chunkID uuid.UUID) (*model.Chunk, bool) {
	return cs.Chunks.Get(chunkID)
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

// ReadChunk reads chunk lenght number of bytes starting at given offset. If lenght is -1 whole chunk file is read
func (cs *ChunkService) ReadChunk(chunkID uuid.UUID, offset, length int) ([]byte, error) {
	chunk, exists := cs.GetChunk(chunkID)
	if !exists {
		return nil, ErrChunkDoesNotExist
	}

	// Read all
	if length == -1 {
		return os.ReadFile(chunk.Path)
	}

	file, err := os.Open(chunk.Path)
	if err != nil {
		return nil, err
	}

	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		return nil, err
	}

	data := make([]byte, length)
	_, err = file.Read(data)
	if err != nil {
		return nil, err
	}

	return data, nil
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
func (c *ChunkServer) IncrementChunkVersion(chunkID uuid.UUID, version int) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	chunk, exists := c.Chunks.Get(chunkID)
	if !exists {
		return ErrChunkDoesNotExist
	}

	newPath := c.GetChunkPath(chunk.ID, chunk.FilePath, chunk.Index, version)
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
	c.Chunks.Set(chunk.ID, *chunk)

	return nil
}
