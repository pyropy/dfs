package master

import (
	"context"
	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/constants"
	"github.com/pyropy/dfs/core/model"
	chunkServerRPC "github.com/pyropy/dfs/rpc/chunkserver"
	"time"
)

type ReplicationMonitor struct {
	leaseStore           *LeaseStore
	chunkMetadataStore   *ChunkMetadataStore
	chunkServerMetaStore *ChunkServerMetadataStore
}

func NewReplicationMonitor(cm *ChunkMetadataStore, lm *LeaseStore, cs *ChunkServerMetadataStore) *ReplicationMonitor {
	return &ReplicationMonitor{
		leaseStore:           lm,
		chunkMetadataStore:   cm,
		chunkServerMetaStore: cs,
	}
}

// Start starts process that monitors all chunks are replicated up to selected replication factor
func (rm *ReplicationMonitor) Start(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	log.Info("starting replication monitor")

	for {
		select {
		case <-ctx.Done():
			return
		case _ = <-ticker.C:
			rm.chunkMetadataStore.Chunks.Range(func(k, v any) bool {
				c := v.(model.ChunkMetadata)
				if len(c.ChunkServers) < constants.REPLICATION_FACTOR {
					err := rm.ReplicateChunk(c.ID)
					if err != nil {
						log.Error(err)
					}
				}

				return true
			})
		default:
		}
	}
}

func (rm *ReplicationMonitor) ReplicateChunk(chunkID uuid.UUID) error {
	chunkMetadata, err := rm.chunkMetadataStore.GetChunk(chunkID)
	if err != nil {
		return err
	}

	if len(chunkMetadata.ChunkServers) == 0 {
		return ErrChunkHasNoHolders
	}

	leaseHolder, leaseHolderExists := rm.leaseStore.GetHolder(chunkID)
	if !leaseHolderExists {
		chunkServerMetadata := rm.chunkServerMetaStore.GetChunkServerMetadata(chunkMetadata.ChunkServers[0])
		leaseHolder = rm.leaseStore.GrantLease(chunkID, chunkServerMetadata)
	}

	replicateFrom := rm.chunkServerMetaStore.GetChunkServerMetadata(leaseHolder.ChunkServerID)
	numberOfReplicas := constants.REPLICATION_FACTOR - len(chunkMetadata.ChunkServers)
	replicateTo := rm.chunkServerMetaStore.SelectChunkServers(numberOfReplicas, chunkMetadata.ChunkServers)

	if len(replicateTo) == 0 {
		return ErrNoChunkServersAvailable
	}

	log.Infow("replication", "status", "replicating chunk", "chunkID", chunkID, "chunkServers", replicateTo)

	return replicateChunk(chunkID, replicateFrom, replicateTo)
}

func replicateChunk(chunkID uuid.UUID, from *ChunkServerMetadata, to []ChunkServerMetadata) error {
	targets := make([]chunkServerRPC.ChunkServer, 0, len(to))
	for _, t := range to {

		target := chunkServerRPC.ChunkServer{
			ID:      t.ID,
			Address: t.Address,
		}

		targets = append(targets, target)
	}

	args := chunkServerRPC.ReplicateChunkArgs{
		ChunkID:      chunkID,
		ChunkServers: targets,
	}

	reply := chunkServerRPC.ReplicateChunkReply{}
	return callChunkServerRPC(from, "ChunkServerAPI.ReplicateChunk", args, &reply)
}
