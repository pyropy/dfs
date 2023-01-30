package model

import "github.com/google/uuid"

type FileMetadata struct {
	ID     uuid.UUID
	Path   string
	Chunks []uuid.UUID
}

type FilePath = string

func NewFileMetadata(path string) FileMetadata {
	return FileMetadata{
		ID:     uuid.New(),
		Path:   path,
		Chunks: []uuid.UUID{},
	}
}
