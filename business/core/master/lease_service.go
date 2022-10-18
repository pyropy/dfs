package master

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type Lease struct {
	ChunkID       uuid.UUID
	ValidUntil    time.Time
	ChunkServerID uuid.UUID
}

// Manages leases
type LeaseService struct {
	Mutex  sync.RWMutex
	Leases map[uuid.UUID]Lease
}

func NewLeaseService() *LeaseService {
	return &LeaseService{
		Leases: map[uuid.UUID]Lease{},
	}
}

// HaveLease checks if chunk servers has lease over chunk for given chunk ID
func (ls *LeaseService) HaveLease(chunkID uuid.UUID) bool {
	lease, leaseExists := ls.Leases[chunkID]
	if !leaseExists {
		return false
	}

	now := time.Now()
	return lease.ValidUntil.After(now)
}

// GrantLease grants lease over chunk for period of time
func (ls *LeaseService) GrantLease(chunkID uuid.UUID, chunkServer *ChunkServerMetadata) *Lease {
	ls.Mutex.Lock()
	defer ls.Mutex.Unlock()

	validFor := 60 * time.Second
	validUntil := time.Now().Add(validFor)

	lease := Lease{
		ChunkID:       chunkID,
		ValidUntil:    validUntil,
		ChunkServerID: chunkServer.ID,
	}

	ls.Leases[chunkID] = lease

	return &lease
}

func (ls *LeaseService) ExtendLease(chunkID ChunkID) (time.Time, error) {
	return time.Now(), nil
}
