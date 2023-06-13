package model

import (
	"github.com/google/uuid"
	"time"
)

type FileMetadata struct {
	ID        uuid.UUID
	Path      string
	Chunks    []uuid.UUID
	Deleted   bool
	DeletedAt time.Time
}

type FilePath = string

func NewFileMetadata(path string) FileMetadata {
	return FileMetadata{
		ID:     uuid.New(),
		Path:   path,
		Chunks: []uuid.UUID{},
	}
}
