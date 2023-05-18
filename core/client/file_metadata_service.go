package client

import (
    "fmt"
	"context"
	"encoding/json"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dslvl "github.com/ipfs/go-ds-leveldb"
	"github.com/pyropy/dfs/core/model"
)



type FileMetadataService struct {
    Files *dslvl.Datastore
}

func NewFileMetadataService(dsPath string) (*FileMetadataService, error) {
    p := fmt.Sprintf("%s/files", dsPath)
    store, err := dslvl.NewDatastore(p, nil)
    if err != nil {
        return nil, err
    }

	return &FileMetadataService{
        Files: store,
	}, nil
}

func (f *FileMetadataService) Get(ctx context.Context, filePath string) (*model.FileMetadata, error) {
    k := ds.NewKey(filePath)
    b, err := f.Files.Get(ctx, k)
	if err != nil {
		return nil, err
	}

    var file model.FileMetadata
    err = json.Unmarshal(b, &file)
	if err != nil {
		return nil, err
	}

	return &file, nil
}

func (f *FileMetadataService) CheckFileExists(ctx context.Context, filePath model.FilePath) (bool, error) {
    k := ds.NewKey(filePath)
    exists, err := f.Files.Has(ctx, k)
    if err != nil {
        return false, err
    }

    return exists, nil
}

func (f *FileMetadataService) AddNewFileMetadata(ctx context.Context, filePath model.FilePath, metadata model.FileMetadata) error {
    b, err := json.Marshal(metadata)
    if err != nil {
        return err
    }

    k := ds.NewKey(filePath)
	return f.Files.Put(ctx, k, b)
}

func (f *FileMetadataService) All(ctx context.Context) ([]*model.FileMetadata, error) {
    q := dsq.Query{}
    files := make([]*model.FileMetadata, 0, 0)

    res, err := f.Files.Query(ctx, q)
    if err != nil {
        return files, err
    }

    for {
        r, hasNext := res.NextSync()
        if !hasNext {
            break
        }

        var file model.FileMetadata
        err = json.Unmarshal(r.Value, &file)
        if err != nil {
            return files, err
        }
        files = append(files, &file)
    }
    
    return files, err
}
