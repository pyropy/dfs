package master

import (
	"errors"
	"github.com/pyropy/dfs/core/constants"
	"github.com/pyropy/dfs/core/model"
	"github.com/pyropy/dfs/lib/logger"
	chunkServerRPC "github.com/pyropy/dfs/rpc/chunkserver"
	"math/rand"
	"net/rpc"
	"time"

	"github.com/google/uuid"
)

type Master struct {
	*LeaseService
	*FileMetadataService
	*ChunkMetadataService
	*ChunkServerMetadataService
}

var (
	ErrFileExists              = errors.New("file exists")
	ErrFileCreation            = errors.New("failed to create file")
	ErrChunkHolderNotFound     = errors.New("chunk holder not found")
	ErrChunkHasNoHolders       = errors.New("chunk has no holders")
	ErrNoChunkServersAvailable = errors.New("no chunk servers available")
)

var log, _ = logger.New("master-rpc")

func NewMaster() *Master {
	return &Master{
		LeaseService:               NewLeaseService(),
		FileMetadataService:        NewFileMetadataService(),
		ChunkMetadataService:       NewChunkMetadataService(),
		ChunkServerMetadataService: NewChunkServerMetadataService(),
	}
}

// CreateNewFile selects chunk servers and instructs them to create N number of chunks with predefined IDs
func (m *Master) CreateNewFile(filePath string, fileSizeBytes, repFactor, chunkSizeBytes int) (*model.FileMetadata, []uuid.UUID, error) {
	// TODO: Add file namespace locks
	var chunkIds []uuid.UUID
	var chunkMetadata []model.ChunkMetadata
	var chunkServerIds []uuid.UUID

	fileExists := m.FileMetadataService.CheckFileExists(filePath)
	if fileExists {
		return nil, chunkIds, ErrFileExists
	}

	chunkVersion := constants.INITIAL_CHUNK_VERSION
	chunkServers := m.ChunkServerMetadataService.SelectChunkServers(repFactor, []uuid.UUID{})
	fileMetadata := model.NewFileMetadata(filePath)
	numChunks := (fileSizeBytes + (chunkSizeBytes - 1)) / chunkSizeBytes

	for _, cs := range chunkServers {
		chunkServerIds = append(chunkServerIds, cs.ID)
	}

	for i := 0; i < numChunks; i++ {
		chunkID := uuid.New()
		chunkIds = append(chunkIds, chunkID)
		fileMetadata.Chunks = append(fileMetadata.Chunks, chunkID)
		chunk := NewChunkMetadata(chunkID, i, chunkVersion, chunkServerIds)
		chunkMetadata = append(chunkMetadata, chunk)

		for _, chunkServer := range chunkServers {
			err := m.createNewChunk(chunkID, filePath, chunkSizeBytes, chunkVersion, &chunkServer)
			if err != nil {
				return nil, nil, ErrFileCreation
			}
		}
	}

	// Add file metadata
	m.FileMetadataService.AddNewFileMetadata(filePath, fileMetadata)

	// Add chunk metadata for each chunk created
	for _, chunkMetadata := range chunkMetadata {
		m.ChunkMetadataService.AddNewChunkMetadata(chunkMetadata)
	}

	return &fileMetadata, chunkServerIds, nil
}

func (m *Master) RequestWrite(chunkID uuid.UUID) (*model.Lease, int, error) {
	chunkServers := m.GetChunkHolders(chunkID)
	if len(chunkServers) == 0 {
		return nil, 0, ErrChunkHolderNotFound
	}

	chunkVersion, err := m.IncrementChunkVersion(chunkID)
	if err != nil {
		return nil, 0, err
	}

	randomIndex := rand.Intn(len(chunkServers))
	chunkServerID := chunkServers[randomIndex]
	chunkServerMetadata := m.GetChunkServerMetadata(chunkServerID)
	lease := m.LeaseService.GrantLease(chunkID, chunkServerMetadata)

	err = m.sendLeaseGrant(chunkID, lease, chunkServerMetadata)
	if err != nil {
		return nil, 0, err
	}

	for _, chunkServerID := range chunkServers {
		chunkServerMetadata := m.GetChunkServerMetadata(chunkServerID)
		// TODO: Retry if fails
		err := m.incrementChunkVersion(chunkID, chunkVersion, chunkServerMetadata)

		if err != nil {
			return nil, 0, err
		}
	}

	return lease, chunkVersion, nil
}

func (m *Master) RequestLeaseRenewal(chunkID uuid.UUID, chunkServer *ChunkServerMetadata) (*model.Lease, error) {
	return m.ExtendLease(chunkID, chunkServer)
}

func (m *Master) StartHealthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	unhealthyChan := make(chan uuid.UUID)

	for {
		select {
		case _ = <-ticker.C:
			m.ChunkServers.Range(func(k any, v any) bool {
				cs := v.(ChunkServerMetadata)

				if !cs.Active {
					return true
				}

				go HealthCheck(cs, unhealthyChan)
				return true

			})
		case unhealthyChunkServerID := <-unhealthyChan:
			cs := m.ChunkServerMetadataService.MarkUnhealthy(unhealthyChunkServerID)
			if !cs.Active {
				log.Warn("health-check", "status", "removed from chunk holders", "chunkID", cs.ID)
				m.ChunkMetadataService.RemoveChunkHolder(cs.ID)
			}
		}
	}
}

// StartReplicationMonitor starts process that monitors all chunks are replicated up to selected replication factor
func (m *Master) StartReplicationMonitor() {
	ticker := time.NewTicker(60 * time.Second)
	log.Info("starting replication monitor")

	for {
		select {
		case _ = <-ticker.C:
			m.ChunkMetadataService.Chunks.Range(func(k, v any) bool {
				c := v.(model.ChunkMetadata)
				if len(c.ChunkServers) < constants.REPLICATION_FACTOR {
					err := m.ReplicateChunk(c.ID)
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

func (m *Master) ReplicateChunk(chunkID uuid.UUID) error {
	chunkMetadata, err := m.ChunkMetadataService.GetChunk(chunkID)
	if err != nil {
		return err
	}

	if len(chunkMetadata.ChunkServers) == 0 {
		return ErrChunkHasNoHolders
	}

	leaseHolder, leaseHolderExists := m.LeaseService.GetHolder(chunkID)
	if !leaseHolderExists {
		chunkServerMetadata := m.ChunkServerMetadataService.GetChunkServerMetadata(chunkMetadata.ChunkServers[0])
		leaseHolder = m.LeaseService.GrantLease(chunkID, chunkServerMetadata)
	}

	replicateFrom := m.ChunkServerMetadataService.GetChunkServerMetadata(leaseHolder.ChunkServerID)
	numberOfReplicas := constants.REPLICATION_FACTOR - len(chunkMetadata.ChunkServers)
	replicateTo := m.ChunkServerMetadataService.SelectChunkServers(numberOfReplicas, chunkMetadata.ChunkServers)

	if len(replicateTo) == 0 {
		return ErrNoChunkServersAvailable
	}

	log.Infow("replication", "status", "replicating chunk", "chunkID", chunkID, "chunkServers", replicateTo)

	return m.replicateChunk(chunkID, replicateFrom, replicateTo)
}

func (m *Master) createNewChunk(id uuid.UUID, filePath string, size int, chunkVersion int, chunkServer *ChunkServerMetadata) error {
	args := chunkServerRPC.CreateChunkRequest{
		ChunkID:      id,
		ChunkSize:    size,
		ChunkVersion: chunkVersion,
		FilePath:     filePath,
	}

	reply := chunkServerRPC.CreateChunkReply{}
	return callChunkServerRPC(chunkServer, "ChunkServerAPI.CreateChunk", args, &reply)
}

func (m *Master) sendLeaseGrant(chunkID uuid.UUID, lease *model.Lease, chunkServer *ChunkServerMetadata) error {
	args := chunkServerRPC.GrantLeaseArgs{
		ChunkID:    chunkID,
		ValidUntil: lease.ValidUntil,
	}

	reply := chunkServerRPC.GrantLeaseReply{}
	return callChunkServerRPC(chunkServer, "ChunkServerAPI.GrantLease", args, &reply)
}

func (m *Master) incrementChunkVersion(chunkID uuid.UUID, version int, chunkServer *ChunkServerMetadata) error {
	args := chunkServerRPC.IncrementChunkVersionArgs{
		ChunkID: chunkID,
		Version: version,
	}
	reply := chunkServerRPC.IncrementChunkVersionReply{}

	return callChunkServerRPC(chunkServer, "ChunkServerAPI.IncrementChunkVersion", args, &reply)
}

func (m *Master) replicateChunk(chunkID uuid.UUID, from *ChunkServerMetadata, to []ChunkServerMetadata) error {
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

func callChunkServerRPC(chunkServer *ChunkServerMetadata, method string, args interface{}, reply interface{}) error {
	client, err := rpc.DialHTTP("tcp", chunkServer.Address)
	if err != nil {
		log.Info("error", chunkServer.Address, "unreachable")
		return err
	}

	defer client.Close()

	err = client.Call(method, args, reply)
	if err != nil {
		log.Info("error", chunkServer.Address, "error", err)
		return err
	}

	return nil
}
