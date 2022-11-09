package master

import (
	"github.com/google/uuid"
	chunkServerRPC "github.com/pyropy/dfs/business/rpc/chunkserver"
	"log"
	"net/rpc"
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

	checkHealth := func(i int) {
		chunkServer := m.Chunkservers[i]
		client, err := rpc.DialHTTP("tcp", chunkServer.Address)
		if err != nil {
			log.Println("error", chunkServer.Address, "unreachable")
			m.Chunkservers[i].Healthy = false
			return
		}

		var reply chunkServerRPC.HealthCheckReply
		args := &chunkServerRPC.HealthCheckArgs{}
		err = client.Call("ChunkServerAPI.HealthCheck", args, &reply)
		if err != nil {
			log.Println("error", chunkServer.Address, "error", err)
			m.Chunkservers[i].Healthy = false
			return
		}

		if reply.Status < 299 {
			m.Chunkservers[i].Healthy = true
			return
		}

		m.Chunkservers[i].Healthy = false
	}

	for {
		select {
		case _ = <-ticker.C:
			for i := 0; i < len(m.Chunkservers); i++ {
				go checkHealth(i)
			}
		}
	}
}
