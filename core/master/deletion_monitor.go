package master

import (
	"context"
	"github.com/pyropy/dfs/core/model"
	"time"
)

type DeletionMonitor struct {
	fileStore *FileMetadataStore
}

var (
	FileDeletionThreshold = time.Second * 86400
)

// NewDeletionMonitor creates new deletion monitor responsible for deleting file metadata
// for files that have been tagged for deletion for more then three days
func NewDeletionMonitor(fileStore *FileMetadataStore) *DeletionMonitor {
	return &DeletionMonitor{
		fileStore: fileStore,
	}
}

// Start starts deletion monitor loop where all files are checked each 60 sec.
// If file is marked for deletion then its sent to deletion channel.
func (gc *DeletionMonitor) Start(ctx context.Context) error {
	ticker := time.NewTicker(60 * time.Second)
	deletionChan := make(chan model.FileMetadata)

	log.Info("starting deletion monitor service")

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			go gc.filterDeletedFiles(deletionChan)
		case f := <-deletionChan:
			if forDeletion := gc.isForDeletion(f); forDeletion {
				gc.fileStore.DeleteFile(f.Path)
			}
		}
	}
}

// filterDeletedFiles filters all files to and sends files marked for deletion to deletion channel
func (gc *DeletionMonitor) filterDeletedFiles(d chan model.FileMetadata) {
	gc.fileStore.Files.Range(func(k, v any) bool {
		f := v.(model.FileMetadata)
		if f.Deleted {
			d <- f
		}

		return true
	})
}

// isForDeletion checks if file has been deleted before now - threshold period
func (gc *DeletionMonitor) isForDeletion(f model.FileMetadata) bool {
	deleteAfter := time.Now().Add(-FileDeletionThreshold)
	return f.Deleted && f.DeletedAt.Before(deleteAfter)
}
