package master

import (
	"sync"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrLeaseNotFound = errors.New("Lease not found")
	ErrLeaseNotPreviouslyOwned = errors.New("Failed to extend lease. Chunk Server was not previous owner of the lease")
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

// ExtendLease extends lease for a given chunkID if chunkserver
// requesting extension previously had lase over the chunk
func (ls *LeaseService) ExtendLease(chunkID uuid.UUID, chunkServer *ChunkServerMetadata) (*Lease, error) {
	prevLease, leaseExists := ls.Leases[chunkID]

	if !leaseExists {
		return nil, ErrLeaseNotFound
	}


	if prevLease.ChunkServerID != chunkServer.ID {
		return nil, ErrLeaseNotPreviouslyOwned

	}

	lease := ls.GrantLease(chunkID, chunkServer)
	return lease, nil
}
