package chunkserver

import (
	"context"
	"log"
	"net/rpc"
	"time"

	"github.com/google/uuid"
	"github.com/pyropy/dfs/rpc/master"
)

type HealthMonitor struct {
	masterAddr    string
	chunkServerID uuid.UUID
	chunkService  *ChunkService
}

func NewHealthMonitor(chunkService *ChunkService) *HealthMonitor {
	return &HealthMonitor{
		chunkService: chunkService,
	}
}

// Start creates ticker that ticks every 10 seconds and triggers ReportHealth func in new goroutine
func (h *HealthMonitor) Start(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ticker.C:
			go h.Report()
		case <-ctx.Done():
			return
		}
	}
}

func (h *HealthMonitor) Report() error {
	if h.masterAddr == "" {
		return nil
	}

	client, err := rpc.DialHTTP("tcp", h.masterAddr)
	if err != nil {
		log.Println("error", "unreachable")
		return err
	}

	defer client.Close()

	chunkReport := make([]master.Chunk, 0)
	for _, chunk := range h.chunkService.GetAllChunks() {
		ch := master.Chunk{
			ID:      chunk.ID,
			Version: chunk.Version,
			Index:   chunk.Index,
		}
		chunkReport = append(chunkReport, ch)
	}

	var reply master.ReportHealthReply
	args := &master.ReportHealthArgs{
		ChunkServerID: h.chunkServerID,
		Chunks:        chunkReport,
	}

	err = client.Call("MasterAPI.ReportHealth", args, &reply)
	if err != nil {
		return err
	}

	return nil
}
