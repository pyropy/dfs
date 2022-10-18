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

// Manages leases
type LeaseService struct {
	Mutex  sync.RWMutex
	Leases map[uuid.UUID]Lease

	leaseExpChan chan *Lease
}

func NewLeaseService(leaseExpChan chan *Lease) *LeaseService {
	return &LeaseService{
		Leases:       map[uuid.UUID]Lease{},
		leaseExpChan: leaseExpChan,
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
func (ls *LeaseService) GrantLease(chunkID uuid.UUID, validUntil time.Time) {
	ls.Mutex.Lock()
	defer ls.Mutex.Unlock()

	lease := Lease{
		ChunkID:    chunkID,
		ValidUntil: validUntil,
	}

	ls.Leases[chunkID] = lease

	go ls.MonitorLease(&lease)
}

// TODO: Make one function that monitors all leases
// MonitorLease computes duration for validity of the lease and waits for it to expire
// Once expired, LeaseService requests lease renewal
func (ls *LeaseService) MonitorLease(lease *Lease) {
	now := time.Now()
	validDuration := lease.ValidUntil.Sub(now)

	timer := time.NewTimer(validDuration)
	// wait for valid for duration
	<-timer.C
	// notify chunk server via channel that lease has expired
	log.Println("lease expired", lease)
	ls.leaseExpChan <- lease
}
