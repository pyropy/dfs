package master

import (
	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/model"
	concurrentMap "github.com/pyropy/dfs/lib/concurrent_map"
	"github.com/pyropy/dfs/lib/utils"
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
	Leases       concurrentMap.Map[uuid.UUID, model.Lease]
	ChunkServers concurrentMap.Map[uuid.UUID, ChunkServerMetadata]
}

func NewChunkServerMetadataService() *ChunkServerMetadataService {
	return &ChunkServerMetadataService{
		Leases:       concurrentMap.NewMap[uuid.UUID, model.Lease](),
		ChunkServers: concurrentMap.NewMap[uuid.UUID, ChunkServerMetadata](),
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

func (m *ChunkServerMetadataService) SelectChunkServers(num int, excludedChunkServers []uuid.UUID) []ChunkServerMetadata {
	result := make([]ChunkServerMetadata, 0, num)
	chunkServers := m.GetAllActiveChunkServers()

	for _, cs := range chunkServers {

		if !utils.Contains(excludedChunkServers, cs.ID) {
			result = append(result, cs)
		}

		if len(result) == num || len(result) == len(chunkServers) {
			return result
		}
	}

	return result[:num-1]
}

func (m *ChunkServerMetadataService) GetChunkServerMetadata(chunkServerID uuid.UUID) *ChunkServerMetadata {
	chunkServerMetadata, exists := m.ChunkServers.Get(chunkServerID)
	if !exists {
		return nil
	}

	return chunkServerMetadata
}

func (m *ChunkServerMetadataService) MarkHealthy(chunkServerID uuid.UUID) *ChunkServerMetadata {
	chunkServer, exists := m.ChunkServers.Get(chunkServerID)
	if !exists {
		return nil
	}

	chunkServer.Healthy = true
	chunkServer.FailedHealthChecks = 0
	chunkServer.Active = true
	m.ChunkServers.Set(chunkServerID, *chunkServer)

	return chunkServer
}

func (m *ChunkServerMetadataService) MarkUnhealthy(chunkServerID uuid.UUID) *ChunkServerMetadata {
	chunkServer, exists := m.ChunkServers.Get(chunkServerID)
	if !exists {
		return nil
	}

	chunkServer.Healthy = false
	chunkServer.FailedHealthChecks += 1
	if chunkServer.FailedHealthChecks >= FailedHealthChecksThreshold {
		chunkServer.Active = false
	}
	m.ChunkServers.Set(chunkServerID, *chunkServer)

	return chunkServer
}
