package master

import (
	"context"
	"github.com/google/uuid"
	"time"
)

type HealthCheckService struct {
	cs *ChunkServerMetadataStore
	cm *ChunkMetadataStore
}

var (
	HealthCheckDurationThreshold = time.Second * 30
)

func NewHealthCheckService(cs *ChunkServerMetadataStore, cm *ChunkMetadataStore) *HealthCheckService {
	return &HealthCheckService{
		cs: cs,
		cm: cm,
	}
}

func (hs *HealthCheckService) Start(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	unhealthyChan := make(chan uuid.UUID)
	log.Info("starting health check service")

	for {
		select {
		case <-ctx.Done():
			return
		case _ = <-ticker.C:
			hs.cs.ChunkServers.Range(func(k any, v any) bool {
				cs := v.(ChunkServerMetadata)

				if !cs.Active {
					return true
				}

				go hs.healthCheck(cs, unhealthyChan)
				return true

			})
		case unhealthyChunkServerID := <-unhealthyChan:
			cs := hs.cs.MarkUnhealthy(unhealthyChunkServerID)
			if !cs.Active {
				log.Warn("health-check", "status", "removed from chunk holders", "chunkID", cs.ID)
				hs.cm.RemoveChunkHolder(cs.ID)
			}
		}
	}
}

func (hs *HealthCheckService) healthCheck(chunkServer ChunkServerMetadata, unhealthy chan uuid.UUID) {
	now := time.Now()
	timeSinceLastReport := now.Sub(chunkServer.LastHealthReport)

	if timeSinceLastReport > HealthCheckDurationThreshold {
		unhealthy <- chunkServer.ID
	}
}
