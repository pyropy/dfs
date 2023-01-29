package master

import (
	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/model"
	"github.com/pyropy/dfs/lib/concurrent_map"
	"time"
)

var (
	FailedHealthChecksThreshold = 3
)

type ChunkServerMetadata struct {
	ID                 uuid.UUID
	Address            string
	Healthy            bool
	Active             bool
	FailedHealthChecks int
	LastHealthReport   time.Time
}

type ChunkServerMetadataService struct {
	Leases       concurrent_map.Map[uuid.UUID, model.Lease]
	ChunkServers concurrent_map.Map[uuid.UUID, ChunkServerMetadata]
}

func NewChunkServerMetadataService() *ChunkServerMetadataService {
	return &ChunkServerMetadataService{
		Leases:       concurrent_map.NewMap[uuid.UUID, model.Lease](),
		ChunkServers: concurrent_map.NewMap[uuid.UUID, ChunkServerMetadata](),
	}
}

func (m *ChunkServerMetadataService) RegisterNewChunkServer(addr string) *ChunkServerMetadata {
	chunkServerMetadata := ChunkServerMetadata{ID: uuid.New(), Address: addr, Healthy: true, Active: true, LastHealthReport: time.Now()}
	m.ChunkServers.Set(chunkServerMetadata.ID, chunkServerMetadata)
	return &chunkServerMetadata
}

func (m *ChunkServerMetadataService) GetAllActiveChunkServers() []ChunkServerMetadata {
	chunkServerList := make([]ChunkServerMetadata, 0)
	m.ChunkServers.Range(func(k, v any) bool {
		cs := v.(ChunkServerMetadata)
		if !cs.Active {
			return true
		}

		chunkServerList = append(chunkServerList, cs)
		return true
	})

	return chunkServerList
}

func (m *ChunkServerMetadataService) SelectChunkServers(num int) []ChunkServerMetadata {
	chunkServers := m.GetAllActiveChunkServers()

	if len(chunkServers) <= num {
		return chunkServers
	}

	return chunkServers[:num-1]
}

func (m *ChunkServerMetadataService) GetChunkServerMetadata(chunkServerID uuid.UUID) *ChunkServerMetadata {
	chunkServerMetadata, exists := m.ChunkServers.Get(chunkServerID)
	if !exists {
		return nil
	}

	return chunkServerMetadata
}

func (m *ChunkServerMetadataService) MarkHealthy(chunkServerID uuid.UUID) {
	chunkServer, exists := m.ChunkServers.Get(chunkServerID)
	if !exists {
		return
	}

	chunkServer.Healthy = true
	chunkServer.FailedHealthChecks = 0
	chunkServer.Active = true
	m.ChunkServers.Set(chunkServerID, *chunkServer)
}

func (m *ChunkServerMetadataService) MarkUnhealthy(chunkServerID uuid.UUID) {
	chunkServer, exists := m.ChunkServers.Get(chunkServerID)
	if !exists {
		return
	}

	chunkServer.Healthy = false
	chunkServer.FailedHealthChecks += 1
	if chunkServer.FailedHealthChecks >= FailedHealthChecksThreshold {
		chunkServer.Active = false
	}
	m.ChunkServers.Set(chunkServerID, *chunkServer)
}
