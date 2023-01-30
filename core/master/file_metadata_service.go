package master

import (
	"github.com/pyropy/dfs/core/model"
	concurrentMap "github.com/pyropy/dfs/lib/concurrent_map"
)

// TODO: Implement some kinda Tree Structure
// to hold file/dir metadata so users can traverse filesystem
type FileMetadataService struct {
	Files concurrentMap.Map[model.FilePath, model.FileMetadata]
}

func NewFileMetadataService() *FileMetadataService {
	return &FileMetadataService{
		Files: concurrentMap.NewMap[model.FilePath, model.FileMetadata](),
	}
}

func (f *FileMetadataService) Get(filePath string) *model.FileMetadata {
	file, exists := f.Files.Get(filePath)
	if !exists {
		return nil
	}

	return file
}

func (f *FileMetadataService) CheckFileExists(filePath model.FilePath) bool {
	_, fileExists := f.Files.Get(filePath)
	return fileExists
}

func (f *FileMetadataService) AddNewFileMetadata(filePath model.FilePath, metadata model.FileMetadata) {
	f.Files.Set(filePath, metadata)
}
