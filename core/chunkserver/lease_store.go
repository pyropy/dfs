package chunkserver

import (
	"github.com/pyropy/dfs/core/model"
	"github.com/pyropy/dfs/lib/cmap"
	"time"

	"github.com/google/uuid"
)

// LeaseStore manages leases
type LeaseStore struct {
	Leases cmap.Map[uuid.UUID, model.Lease]
}

func NewLeaseStore() *LeaseStore {
	return &LeaseStore{
		Leases: cmap.NewMap[uuid.UUID, model.Lease](),
	}
}

// HasLease checks if chunk servers has lease over chunk for given chunk ID
func (ls *LeaseStore) HasLease(chunkID uuid.UUID) bool {
	lease, leaseExists := ls.Leases.Get(chunkID) // ls.Leases[chunkID]
	if !leaseExists {
		return false
	}

	return lease.IsExpired()
}

// GrantLease grants lease over chunk for period of time
func (ls *LeaseStore) GrantLease(chunkID uuid.UUID, validUntil time.Time) {
	lease := model.Lease{
		ChunkID:    chunkID,
		ValidUntil: validUntil,
	}

	ls.Leases.Set(chunkID, lease)
}
