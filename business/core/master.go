package core

import (
	"sync"
)

type ChunkserverMetadata struct {
	ID      int
	Address string
	Healthy bool
}

type ChunkID = int

type ChunkMetadata struct {
	ID           int
	Version      int
	Chunkservers []int
	Lease        int
	Checksum     int
}

type FileMetadata struct {
	ID     int
	Path   string
	Chunks []ChunkID
}

type Master struct {
	Mutex        sync.Mutex
	Chunkservers []ChunkserverMetadata
	Files        []FileMetadata
	Chunks       []ChunkMetadata
}

func (m *Master) RegisterChunkServer(addr string) *ChunkserverMetadata {
	chunkServerMetadata := ChunkserverMetadata{ID: len(m.Chunkservers) + 1, Address: addr, Healthy: true}
	return &chunkServerMetadata
}
