package chunkserver

import (
	"context"
	"github.com/google/uuid"
	"github.com/pyropy/dfs/core/model"
	"github.com/pyropy/dfs/rpc/master"
	"log"
	"net/rpc"
	"time"
)

type LeaseMonitorService struct {
	masterAddr    string
	chunkServerID uuid.UUID
	leaseExpChan  chan model.Lease
	leaseStore    *LeaseStore
}

func NewLeaseMonitor(leaseStore *LeaseStore, leaseExpChan chan model.Lease) *LeaseMonitorService {
	return &LeaseMonitorService{
		leaseStore:   leaseStore,
		leaseExpChan: leaseExpChan,
	}
}

func (l *LeaseMonitorService) Start(ctx context.Context) {
	go l.MonitorLeases(ctx)

	for {
		select {
		case lease := <-l.leaseExpChan:
			err := l.RequestLeaseRenewal(lease)
			if err != nil {
				log.Println("error", "chunkServer", "lease renewal failed", lease, err)
			}
		case <-ctx.Done():
			log.Println("shutdown", "leaseMonitorService", "shutting down lease monitor service")
			return
		default:
		}
	}

}

func (l *LeaseMonitorService) MonitorLeases(ctx context.Context) {
	// loops every 100ms or until canceled
	for {
		select {
		case <-time.After(time.Millisecond * 100):
			l.leaseStore.Leases.Range(func(k, v any) bool {
				id := k.(uuid.UUID)
				lease := v.(model.Lease)
				if lease.IsExpired() {
					l.leaseStore.Leases.Delete(id)
					log.Println("debug", "lease service", "lease expired", lease)
					l.leaseExpChan <- lease
				}

				return true
			})
		case <-ctx.Done():
			close(l.leaseExpChan)
			return
		}
	}
}

// RequestLeaseRenewal requests renewal for given lease from master
func (l *LeaseMonitorService) RequestLeaseRenewal(lease model.Lease) error {
	client, err := rpc.DialHTTP("tcp", l.masterAddr)
	if err != nil {
		log.Println("error", "unreachable")
		return err
	}

	var reply master.RequestLeaseRenewalReply
	args := &master.RequestLeaseRenewalArgs{
		ChunkID:       lease.ChunkID,
		ChunkServerID: l.chunkServerID,
	}

	err = client.Call("MasterAPI.RequestLeaseRenewal", args, &reply)
	if err != nil {
		return err
	}

	if !reply.Granted {
		return ErrChunkLeaseNotGranted
	}

	l.leaseStore.GrantLease(reply.ChunkID, reply.ValidUntil)

	log.Println("info", "chunkServer", "lease granted", reply.ChunkID, reply.ValidUntil)
	return nil
}
