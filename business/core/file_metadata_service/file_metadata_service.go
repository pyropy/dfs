package filemetadataservice

import (
	"sync"

	"github.com/google/uuid"
)

type FileMetadata struct {
	ID     uuid.UUID
	Path   string
	Chunks []uuid.UUID
}

type FilePath = string

type FileMetadataService struct {
	Mutex sync.RWMutex
	Files map[FilePath]FileMetadata
}

func NewFileMetadataService() *FileMetadataService {
	return &FileMetadataService{
		Files: map[string]FileMetadata{},
	}
}

func (f *FileMetadataService) CheckFileExists(filePath FilePath) bool {
	f.Mutex.RLock()
	defer f.Mutex.RUnlock()

	_, fileExists := f.Files[filePath]
	return fileExists
}

func (f *FileMetadataService) AddNewFileMetadata(filePath FilePath, metadata FileMetadata) {
	f.Mutex.Lock()
	defer f.Mutex.Unlock()

	f.Files[filePath] = metadata
}

func NewFileMetadata(path string) FileMetadata {
	return FileMetadata{
		ID:     uuid.New(),
		Path:   path,
		Chunks: []uuid.UUID{},
	}
}
