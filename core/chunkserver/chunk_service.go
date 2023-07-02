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
	"sync"
)

type ChunkService struct {
	Cfg    *Config
	Lock   sync.RWMutex
	Chunks cmap.Map[uuid.UUID, model.Chunk]
}

var (
	ErrChunkDoesNotExist = errors.New("chunk does not exist")
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

func (c *ChunkService) GetChunkPath(id uuid.UUID, filePath string, index, version int) string {
	filename := GetChunkFilename(id, index, version)
	filepath := fp.Join(c.Cfg.Chunks.Path, filePath, filename)

	return filepath
}

func (c *ChunkService) CreateChunk(id uuid.UUID, filePath string, index, version, size int) (*model.Chunk, error) {
	// create chunks parent path dir
	chunk := model.Chunk{
		ID:       id,
		Version:  version,
		FilePath: filePath,
	}

	c.Lock.Lock()
	defer c.Lock.Unlock()

	chunksParentPath := fp.Join(c.Cfg.Chunks.Path, filePath)
	err := os.MkdirAll(chunksParentPath, 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	chunkPath := c.GetChunkPath(id, filePath, index, version)
	_, err = os.Create(chunkPath)
	if err != nil {
		return nil, err
	}

	chunk.Path = chunkPath

	c.AddChunk(chunk)

	return &chunk, nil
}

func (c *ChunkService) AddChunk(chunk model.Chunk) {
	c.Chunks.Set(chunk.ID, chunk)
}

func (c *ChunkService) GetChunk(chunkID uuid.UUID) (*model.Chunk, bool) {
	return c.Chunks.Get(chunkID)
}

func (c *ChunkService) GetAllChunks() []model.Chunk {
	chunks := make([]model.Chunk, 0)

	c.Chunks.Range(func(k, v any) bool {
		chunk := v.(model.Chunk)
		chunks = append(chunks, chunk)

		return true
	})

	return chunks
}

// ReadChunk reads chunk length number of bytes starting at given offset. If length is -1 whole chunk file is read
func (c *ChunkService) ReadChunk(chunkID uuid.UUID, offset, length int) ([]byte, error) {
	chunk, exists := c.GetChunk(chunkID)
	if !exists {
		return nil, ErrChunkDoesNotExist
	}

	c.Lock.RLock()
	defer c.Lock.RUnlock()

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

func (c *ChunkService) WriteChunkBytes(chunkID uuid.UUID, data []byte, offset int, version int) (int, error) {
	chunk, exists := c.GetChunk(chunkID)
	if !exists {
		return 0, ErrChunkDoesNotExist
	}

	c.Lock.Lock()
	defer c.Lock.Lock()

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
func (c *ChunkService) IncrementChunkVersion(chunkID uuid.UUID, version int) error {
	chunk, exists := c.Chunks.Get(chunkID)
	if !exists {
		return ErrChunkDoesNotExist
	}

	c.Lock.Lock()
	defer c.Lock.Unlock()

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

func (c *ChunkService) DeleteChunk(chunkID uuid.UUID) error {
	chunk, exists := c.Chunks.Get(chunkID)
	if !exists {
		return ErrChunkDoesNotExist
	}

	c.Lock.Lock()
	defer c.Lock.Unlock()

	if err := os.Remove(chunk.Path); err != nil {
		return err
	}

	c.Chunks.Delete(chunkID)
	return nil
}
