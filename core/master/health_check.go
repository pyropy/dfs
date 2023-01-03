package master

import (
	"github.com/google/uuid"
	"time"
)

var (
	HealthCheckDurationThreshold = time.Second * 30
)

func HealthCheck(chunkServer ChunkServerMetadata, unhealthy chan uuid.UUID) {
	now := time.Now()
	timeSinceLastReport := now.Sub(chunkServer.LastHealthReport)

	if timeSinceLastReport > HealthCheckDurationThreshold {
		unhealthy <- chunkServer.ID
	}
}
