package chunkserver

import (
	"github.com/pyropy/dfs/core/model"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

// LeaseService manages leases
type LeaseService struct {
	Mutex sync.RWMutex

	Leases map[uuid.UUID]model.Lease

	leaseExpChan chan *model.Lease
}

func NewLeaseService(leaseExpChan chan *model.Lease) *LeaseService {
	return &LeaseService{
		Leases:       map[uuid.UUID]model.Lease{},
		leaseExpChan: leaseExpChan,
	}
}

// HaveLease checks if chunk servers has lease over chunk for given chunk ID
func (ls *LeaseService) HaveLease(chunkID uuid.UUID) bool {
	ls.Mutex.RLock()
	defer ls.Mutex.RUnlock()

	lease, leaseExists := ls.Leases[chunkID]
	if !leaseExists {
		return false
	}

	return lease.IsExpired()
}

// GrantLease grants lease over chunk for period of time
func (ls *LeaseService) GrantLease(chunkID uuid.UUID, validUntil time.Time) {
	ls.Mutex.Lock()
	defer ls.Mutex.Unlock()

	lease := model.Lease{
		ChunkID:    chunkID,
		ValidUntil: validUntil,
	}

	ls.Leases[chunkID] = lease
}

// MonitorLeases loops over all leases and checks if they are expired
// Once expired, LeaseService requests lease renewal
func (ls *LeaseService) MonitorLeases() {
	// we never gonna die
	for {
		for k := range ls.Leases {
			lease := ls.Leases[k]
			if lease.IsExpired() {
				ls.Mutex.Lock()
				log.Println("debug", "lease service", "lease expired", lease)
				delete(ls.Leases, k)
				ls.leaseExpChan <- &lease
				ls.Mutex.Unlock()
			}
		}
	}
}
