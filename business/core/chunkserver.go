package core

import (
	"errors"
	"fmt"
	"os"
	fp "path/filepath"
)

// TODO: Implement some kinda Tree Structure
// to hold file/dir metadata so users can traverse filesystem

type Chunk struct {
	ID       int
	Version  int
	Path     string // disk path
	Checksum int
}

type ChunkServer struct {
	Chunks map[int]Chunk
}

var (
	ErrChunkAlreadyExists = errors.New("Chunk already exists")
)

func (c *ChunkServer) CreateChunk(id, version int) (*Chunk, error) {
	existingChunk, exists := c.Chunks[id]

	if exists && existingChunk.Version == version {
		return nil, ErrChunkAlreadyExists
	}

	filename := fmt.Sprintf("%d-%d", id, version)
	filepath := fp.Join("chunks", filename)

	_, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}

	chunk := Chunk{
		ID:      id,
		Version: version,
		Path:    filepath,
	}

	c.Chunks[id] = chunk

	return &chunk, nil
}
