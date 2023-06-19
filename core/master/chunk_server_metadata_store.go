package master

import (
	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/model"
	"github.com/pyropy/dfs/lib/cmap"
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

type ChunkServerMetadataStore struct {
	Leases       cmap.Map[uuid.UUID, model.Lease]
	ChunkServers cmap.Map[uuid.UUID, ChunkServerMetadata]
}

func NewChunkServerMetadataStore() *ChunkServerMetadataStore {
	return &ChunkServerMetadataStore{
		Leases:       cmap.NewMap[uuid.UUID, model.Lease](),
		ChunkServers: cmap.NewMap[uuid.UUID, ChunkServerMetadata](),
	}
}

func (m *ChunkServerMetadataStore) RegisterNewChunkServer(addr string) *ChunkServerMetadata {
	chunkServerMetadata := ChunkServerMetadata{ID: uuid.New(), Address: addr, Healthy: true, Active: true, LastHealthReport: time.Now()}
	m.ChunkServers.Set(chunkServerMetadata.ID, chunkServerMetadata)
	return &chunkServerMetadata
}

func (m *ChunkServerMetadataStore) GetAllActiveChunkServers() []ChunkServerMetadata {
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

func (m *ChunkServerMetadataStore) SelectChunkServers(num int, excludedChunkServers []uuid.UUID) []ChunkServerMetadata {
	// TODO: Optimise by implementing some kinda priority queue
	result := make([]ChunkServerMetadata, 0)
	chunkServers := m.GetAllActiveChunkServers()

	for _, cs := range chunkServers {

		if !utils.Contains(excludedChunkServers, cs.ID) {
			result = append(result, cs)
		}

		if len(result) == num || len(result) == len(chunkServers) {
			return result
		}
	}

	if len(result) > num {
		return result[:num-1]
	}

	return result
}

func (m *ChunkServerMetadataStore) GetChunkServerMetadata(chunkServerID uuid.UUID) *ChunkServerMetadata {
	chunkServerMetadata, exists := m.ChunkServers.Get(chunkServerID)
	if !exists {
		return nil
	}

	return chunkServerMetadata
}

func (m *ChunkServerMetadataStore) MarkHealthy(chunkServerID uuid.UUID) *ChunkServerMetadata {
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

func (m *ChunkServerMetadataStore) MarkUnhealthy(chunkServerID uuid.UUID) *ChunkServerMetadata {
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
