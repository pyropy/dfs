package chunkserver

import (
	"github.com/pyropy/dfs/core/model"
	"github.com/pyropy/dfs/lib/cmap"
	"log"
	"time"

	"github.com/google/uuid"
)

// LeaseService manages leases
type LeaseService struct {
	Leases cmap.Map[uuid.UUID, model.Lease]

	leaseExpChan chan *model.Lease
}

func NewLeaseService(leaseExpChan chan *model.Lease) *LeaseService {
	return &LeaseService{
		Leases:       cmap.NewMap[uuid.UUID, model.Lease](),
		leaseExpChan: leaseExpChan,
	}
}

// HaveLease checks if chunk servers has lease over chunk for given chunk ID
func (ls *LeaseService) HaveLease(chunkID uuid.UUID) bool {
	lease, leaseExists := ls.Leases.Get(chunkID) // ls.Leases[chunkID]
	if !leaseExists {
		return false
	}

	return lease.IsExpired()
}

// GrantLease grants lease over chunk for period of time
func (ls *LeaseService) GrantLease(chunkID uuid.UUID, validUntil time.Time) {
	lease := model.Lease{
		ChunkID:    chunkID,
		ValidUntil: validUntil,
	}

	ls.Leases.Set(chunkID, lease)
}

// MonitorLeases loops over all leases and checks if they are expired
// Once expired, LeaseService requests lease renewal
func (ls *LeaseService) MonitorLeases() {
	// we never gonna die
	for {
		ls.Leases.Range(func(k, v any) bool {
			id := k.(uuid.UUID)
			lease := v.(model.Lease)
			if lease.IsExpired() {
				ls.Leases.Delete(id)
				log.Println("debug", "lease service", "lease expired", lease)
				ls.leaseExpChan <- &lease
			}

			return true
		})
	}
}
