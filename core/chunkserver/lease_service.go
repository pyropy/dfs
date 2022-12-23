package chunkserver

import (
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Lease struct {
	ChunkID    uuid.UUID
	ValidUntil time.Time
}

// LeaseService manages leases
type LeaseService struct {
	Mutex sync.RWMutex

	Leases map[uuid.UUID]Lease

	leaseExpChan chan *Lease
}

func (l *Lease) IsExpired() bool {
	now := time.Now()

	return !l.ValidUntil.After(now)
}

func NewLeaseService(leaseExpChan chan *Lease) *LeaseService {
	return &LeaseService{
		Leases:       map[uuid.UUID]Lease{},
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

	lease := Lease{
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
