package chunkserver

import (
	"errors"
	"fmt"
	"log"
	"os"
	fp "path/filepath"
	"sync"

	"github.com/google/uuid"
)

type Chunk struct {
	ID       uuid.UUID
	Version  int
	Path     string // chunk path on disk
	FilePath string // file path on disk
	Checksum int
	Index    int
}

type ChunkService struct {
	Mutex  sync.RWMutex
	Chunks map[uuid.UUID]Chunk
}

var (
	ErrChunkDoesNotExist = errors.New("Chunk does not exist")
)

func NewChunkService() *ChunkService {
	return &ChunkService{
		Chunks: map[uuid.UUID]Chunk{},
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

func (cs *ChunkService) CreateChunk(id uuid.UUID, filePath string, index, version, size int) (*Chunk, error) {
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

	chunk := Chunk{
		ID:       id,
		Version:  version,
		Path:     chunkPath,
		FilePath: filePath,
	}

	return &chunk, nil
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

func (cs *ChunkService) GetAllChunks() []Chunk {
	cs.Mutex.RLock()
	defer cs.Mutex.RUnlock()
	chunks := make([]Chunk, 0, len(cs.Chunks))

	for _, chunk := range cs.Chunks {
		chunks = append(chunks, chunk)
	}

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

	chunk, exists := cs.Chunks[chunkID]
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
	cs.Chunks[chunk.ID] = chunk

	return nil
}
