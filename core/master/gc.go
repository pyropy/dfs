package master

import (
	"context"
	"fmt"
	"github.com/pyropy/dfs/core/model"
	"time"
)

type GC struct {
	fileStore *FileMetadataStore
}

var (
	DeletionThreshold = time.Second * 86400
)

func NewGC(fileStore *FileMetadataStore) *GC {
	return &GC{
		fileStore: fileStore,
	}
}

// Start starts GC loop where all files are checked each 60 sec.
// If file is marked for deletion then its sent to deletion channel.
func (gc *GC) Start(ctx context.Context) error {
	ticker := time.NewTicker(60 * time.Second)
	deletionChan := make(chan model.FileMetadata)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			go gc.filterDeletedFiles(deletionChan)
		case f := <-deletionChan:
			shouldDelete := gc.isForDeletion(f)
			if shouldDelete {
				go gc.sweep(f)
			}
		}
	}
}

// filterDeletedFiles filters all files to and sends files marked for deletion to deletion channel
func (gc *GC) filterDeletedFiles(d chan model.FileMetadata) {
	gc.fileStore.Files.Range(func(k, v any) bool {
		f := v.(model.FileMetadata)
		if f.Deleted {
			d <- f
		}

		return true
	})
}

// isForDeletion checks if file has been deleted before now - threshold period
func (gc *GC) isForDeletion(f model.FileMetadata) bool {
	deleteAfter := time.Now().Add(-DeletionThreshold)
	return f.Deleted && f.DeletedAt.Before(deleteAfter)
}

// sweep performs delete of file
func (gc *GC) sweep(f model.FileMetadata) {
	// TODO: get chunk holders for each chunk and send them deletion request
	for _, c := range f.Chunks {
		fmt.Println(c)
	}
}
