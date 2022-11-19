package master

import (
	"github.com/google/uuid"
	"sync"
	"time"
)

type ChunkServerMetadata struct {
	ID      uuid.UUID
	Address string
	Healthy bool
}

type ChunkServerMetadataService struct {
	Mutex        sync.RWMutex
	Leases       map[uuid.UUID]Lease
	Chunkservers []ChunkServerMetadata
}

func NewChunkServerMetadataService() *ChunkServerMetadataService {
	return &ChunkServerMetadataService{
		Leases:       map[uuid.UUID]Lease{},
		Chunkservers: []ChunkServerMetadata{},
	}
}

func (m *ChunkServerMetadataService) RegisterNewChunkServer(addr string) *ChunkServerMetadata {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	chunkServerMetadata := ChunkServerMetadata{ID: uuid.New(), Address: addr, Healthy: true}
	m.Chunkservers = append(m.Chunkservers, chunkServerMetadata)
	return &chunkServerMetadata
}

func (m *ChunkServerMetadataService) SelectChunkServers(num int) []ChunkServerMetadata {
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()

	if len(m.Chunkservers) <= num {
		return m.Chunkservers
	}

	return m.Chunkservers[:num-1]
}

func (m *ChunkServerMetadataService) GetChunkServerMetadata(chunkServerID uuid.UUID) *ChunkServerMetadata {
	var chunkServerMetadata ChunkServerMetadata

	m.Mutex.RLock()
	defer m.Mutex.RUnlock()

	for _, cs := range m.Chunkservers {
		if cs.ID == chunkServerID {
			chunkServerMetadata = cs
		}
	}

	return &chunkServerMetadata
}

// TODO: Create special service for health checks
func (m *ChunkServerMetadataService) StartHealthCheckService() {
	ticker := time.NewTicker(30 * time.Second)
	unhealthyChan := make(chan uuid.UUID)

	for {
		select {
		case _ = <-ticker.C:
			for i := 0; i < len(m.Chunkservers); i++ {
				cs := m.Chunkservers[i]
				go HealthCheck(cs, unhealthyChan)
			}
		case unhealthyChunkServerID := <-unhealthyChan:
			m.MarkUnhealthy(unhealthyChunkServerID)
		}
	}
}

func (m *ChunkServerMetadataService) MarkUnhealthy(chunkServerID uuid.UUID) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	for i, cs := range m.Chunkservers {
		if cs.ID == chunkServerID {
			m.Chunkservers[i].Healthy = false
		}
	}
}
