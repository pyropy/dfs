package master

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/constants"
	"github.com/pyropy/dfs/core/model"
	"github.com/pyropy/dfs/lib/logger"
	"math/rand"
)

type Master struct {
	*LeaseStore
	*FileMetadataStore
	*ChunkMetadataStore
	*ChunkServerMetadataStore
	*GC
	*HealthCheckService
	*DeletionMonitor
	*ReplicationMonitor
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
	chunkMetadataStore := NewChunkMetadataStore()
	chunkServerMetadataStore := NewChunkServerMetadataStore()
	leaseService := NewLeaseStore()
	fileMetadataStore := NewFileMetadataStore()

	return &Master{
		LeaseStore:               leaseService,
		FileMetadataStore:        fileMetadataStore,
		ChunkMetadataStore:       chunkMetadataStore,
		ChunkServerMetadataStore: chunkServerMetadataStore,
		GC:                       NewGC(fileMetadataStore, chunkMetadataStore, chunkServerMetadataStore),
		HealthCheckService:       NewHealthCheckService(chunkServerMetadataStore, chunkMetadataStore),
		DeletionMonitor:          NewDeletionMonitor(fileMetadataStore),
		ReplicationMonitor:       NewReplicationMonitor(chunkMetadataStore, leaseService, chunkServerMetadataStore),
	}
}

// CreateNewFile selects chunk servers and instructs them to create N number of chunks with predefined IDs
func (m *Master) CreateNewFile(filePath string, fileSizeBytes, repFactor, chunkSizeBytes int) (*model.FileMetadata, []uuid.UUID, error) {
	// TODO: Add file namespace locks
	var chunkIds []uuid.UUID
	var chunkMetadata []model.ChunkMetadata
	var chunkServerIds []uuid.UUID

	fileExists := m.FileMetadataStore.CheckFileExists(filePath)
	if fileExists {
		return nil, chunkIds, ErrFileExists
	}

	chunkVersion := constants.INITIAL_CHUNK_VERSION
	chunkServers := m.ChunkServerMetadataStore.SelectChunkServers(repFactor, []uuid.UUID{})
	fileMetadata := model.NewFileMetadata(filePath)
	numChunks := (fileSizeBytes + (chunkSizeBytes - 1)) / chunkSizeBytes

	for _, cs := range chunkServers {
		chunkServerIds = append(chunkServerIds, cs.ID)
	}

	for i := 0; i < numChunks; i++ {
		chunkID := uuid.New()
		chunkIds = append(chunkIds, chunkID)
		fileMetadata.Chunks = append(fileMetadata.Chunks, chunkID)
		chunk := NewChunkMetadata(chunkID, i, chunkVersion, filePath, chunkServerIds)
		chunkMetadata = append(chunkMetadata, chunk)

		for _, chunkServer := range chunkServers {
			err := createNewChunk(chunkID, filePath, chunkSizeBytes, chunkVersion, &chunkServer)
			if err != nil {
				return nil, nil, ErrFileCreation
			}
		}
	}

	// Add file metadata
	m.FileMetadataStore.AddNewFileMetadata(filePath, fileMetadata)

	// Add chunk metadata for each chunk created
	for _, meta := range chunkMetadata {
		m.ChunkMetadataStore.AddNewChunkMetadata(meta)
	}

	return &fileMetadata, chunkServerIds, nil
}

func (m *Master) RequestWrite(chunkID uuid.UUID) (uuid.UUID, *model.Lease, []*ChunkServerMetadata, int, error) {
	chunkServerIds := m.GetChunkHolders(chunkID)
	if len(chunkServerIds) == 0 {
		return uuid.UUID{}, nil, nil, 0, ErrChunkHolderNotFound
	}

	chunkServers := make([]*ChunkServerMetadata, 0)
	for _, chunkServerId := range chunkServerIds {
		chunkServer := m.ChunkServerMetadataStore.GetChunkServerMetadata(chunkServerId)
		chunkServers = append(chunkServers, chunkServer)
	}

	lease, hasLeaseHolder := m.LeaseStore.GetHolder(chunkID)

	chunkVersion, err := m.IncrementChunkVersion(chunkID)
	if err != nil {
		return uuid.UUID{}, nil, nil, 0, err
	}

	if !hasLeaseHolder {
		lease, err = m.grantLeaseRandom(chunkID, chunkServers)
		if err != nil {
			return uuid.UUID{}, nil, nil, 0, err
		}
	}

	for _, chunkServer := range chunkServers {
		// TODO: Retry if fails
		err = incrementChunkVersion(chunkID, chunkVersion, chunkServer)
		if err != nil {
			return uuid.UUID{}, nil, nil, 0, err
		}
	}

	return chunkID, lease, chunkServers, chunkVersion, nil
}

func (m *Master) RequestLeaseRenewal(chunkID uuid.UUID, chunkServer *ChunkServerMetadata) (*model.Lease, error) {
	return m.LeaseStore.ExtendLease(chunkID, chunkServer)
}

func (m *Master) StartHealthCheck(ctx context.Context) {
	m.HealthCheckService.Start(ctx)
}

func (m *Master) StartDeletionMonitor(ctx context.Context) {
	m.DeletionMonitor.Start(ctx)
}

func (m *Master) StartReplicationMonitor(ctx context.Context) {
	m.ReplicationMonitor.Start(ctx)
}

func (m *Master) StartGC(ctx context.Context) {
	m.GC.Start(ctx)
}

func (m *Master) grantLeaseRandom(chunkID uuid.UUID, chunkServers []*ChunkServerMetadata) (*model.Lease, error) {
	randomIndex := rand.Intn(len(chunkServers))
	chunkServerMetadata := chunkServers[randomIndex]
	lease := m.LeaseStore.GrantLease(chunkID, chunkServerMetadata)
	err := sendLeaseGrant(chunkID, lease, chunkServerMetadata)
	return lease, err
}
