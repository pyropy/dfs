package master

import (
	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/lease_service"
	"sync"
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
	Mutex        sync.RWMutex
	Leases       map[uuid.UUID]lease_service.Lease
	ChunkServers map[uuid.UUID]ChunkServerMetadata
}

func NewChunkServerMetadataService() *ChunkServerMetadataService {
	return &ChunkServerMetadataService{
		Leases:       map[uuid.UUID]lease_service.Lease{},
		ChunkServers: map[uuid.UUID]ChunkServerMetadata{},
	}
}

func (m *ChunkServerMetadataService) RegisterNewChunkServer(addr string) *ChunkServerMetadata {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	chunkServerMetadata := ChunkServerMetadata{ID: uuid.New(), Address: addr, Healthy: true, Active: true, LastHealthReport: time.Now()}
	m.ChunkServers[chunkServerMetadata.ID] = chunkServerMetadata
	return &chunkServerMetadata
}

func (m *ChunkServerMetadataService) GetAllActiveChunkServers() []ChunkServerMetadata {
	chunkServerList := make([]ChunkServerMetadata, 0, len(m.ChunkServers))
	for _, cs := range m.ChunkServers {
		if !cs.Active {
			continue
		}

		chunkServerList = append(chunkServerList, cs)
	}

	return chunkServerList
}

func (m *ChunkServerMetadataService) SelectChunkServers(num int) []ChunkServerMetadata {
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()

	chunkServers := m.GetAllActiveChunkServers()

	if len(chunkServers) <= num {
		return chunkServers
	}

	return chunkServers[:num-1]
}

func (m *ChunkServerMetadataService) GetChunkServerMetadata(chunkServerID uuid.UUID) *ChunkServerMetadata {
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()

	chunkServerMetadata := m.ChunkServers[chunkServerID]
	return &chunkServerMetadata
}

func (m *ChunkServerMetadataService) MarkHealthy(chunkServerID uuid.UUID) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	chunkServer := m.ChunkServers[chunkServerID]
	chunkServer.Healthy = true
	chunkServer.FailedHealthChecks = 0
	chunkServer.Active = true
	m.ChunkServers[chunkServerID] = chunkServer
}

func (m *ChunkServerMetadataService) MarkUnhealthy(chunkServerID uuid.UUID) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	chunkServer := m.ChunkServers[chunkServerID]
	chunkServer.Healthy = false
	chunkServer.FailedHealthChecks += 1
	if chunkServer.FailedHealthChecks >= FailedHealthChecksThreshold {
		chunkServer.Active = false
	}
	m.ChunkServers[chunkServerID] = chunkServer
}
