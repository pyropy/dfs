package master

import (
	"github.com/pyropy/dfs/core/model"
	"github.com/pyropy/dfs/lib/cmap"
)

// TODO: Implement some kinda Tree Structure
// to hold file/dir metadata so users can traverse filesystem
type FileMetadataStore struct {
	Files cmap.Map[model.FilePath, model.FileMetadata]
}

func NewFileMetadataStore() *FileMetadataStore {
	return &FileMetadataStore{
		Files: cmap.NewMap[model.FilePath, model.FileMetadata](),
	}
}

func (f *FileMetadataStore) Get(filePath string) *model.FileMetadata {
	file, exists := f.Files.Get(filePath)
	if !exists {
		return nil
	}

	return file
}

func (f *FileMetadataStore) CheckFileExists(filePath model.FilePath) bool {
	_, fileExists := f.Files.Get(filePath)
	return fileExists
}

func (f *FileMetadataStore) AddNewFileMetadata(filePath model.FilePath, metadata model.FileMetadata) {
	f.Files.Set(filePath, metadata)
}

func (f *FileMetadataStore) DeleteFile(filePath model.FilePath) {
	f.Files.Delete(filePath)
}
