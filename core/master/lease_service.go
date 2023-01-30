package master

import (
	"errors"
	"github.com/pyropy/dfs/core/model"
	"github.com/pyropy/dfs/lib/cmap"
	"time"

	"github.com/google/uuid"
)

var (
	ErrLeaseNotFound           = errors.New("Lease not found")
	ErrLeaseNotPreviouslyOwned = errors.New("Failed to extend lease. Chunk Server was not previous owner of the lease")
)

// Manages leases
type LeaseService struct {
	Leases cmap.Map[uuid.UUID, model.Lease]
}

func NewLeaseService() *LeaseService {
	return &LeaseService{
		Leases: cmap.NewMap[uuid.UUID, model.Lease](),
	}
}

// GetHolder returns lease holder for chunk id if any
func (ls *LeaseService) GetHolder(chunkID uuid.UUID) (*model.Lease, bool) {
	return ls.Leases.Get(chunkID)
}

// HaveLease checks if chunk servers has lease over chunk for given chunk ID
func (ls *LeaseService) HaveLease(chunkID uuid.UUID) bool {
	lease, leaseExists := ls.Leases.Get(chunkID)
	if !leaseExists {
		return false
	}

	now := time.Now()
	return lease.ValidUntil.After(now)
}

// GrantLease grants lease over chunk for period of time
func (ls *LeaseService) GrantLease(chunkID uuid.UUID, chunkServer *ChunkServerMetadata) *model.Lease {
	validFor := 60 * time.Second
	validUntil := time.Now().Add(validFor)

	lease := model.Lease{
		ChunkID:       chunkID,
		ValidUntil:    validUntil,
		ChunkServerID: chunkServer.ID,
	}

	ls.Leases.Set(chunkID, lease)
	return &lease
}

// ExtendLease extends lease for a given chunkID if chunkserver
// requesting extension previously had lase over the chunk
func (ls *LeaseService) ExtendLease(chunkID uuid.UUID, chunkServer *ChunkServerMetadata) (*model.Lease, error) {
	prevLease, leaseExists := ls.Leases.Get(chunkID)

	if !leaseExists {
		return nil, ErrLeaseNotFound
	}

	if prevLease.ChunkServerID != chunkServer.ID {
		return nil, ErrLeaseNotPreviouslyOwned

	}

	lease := ls.GrantLease(chunkID, chunkServer)
	return lease, nil
}
