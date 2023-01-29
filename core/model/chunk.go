package model

import "github.com/google/uuid"

type Chunk struct {
	ID       uuid.UUID
	Version  int
	Path     string // chunk path on disk
	FilePath string // file path on disk
	Checksum int
	Index    int
}

type ChunkMetadata struct {
	Chunk
	ChunkServers []uuid.UUID
	Lease        uuid.UUID
}
