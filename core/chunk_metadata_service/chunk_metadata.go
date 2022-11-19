package chunkmetadataservice

import "github.com/google/uuid"

type ChunkMetadata struct {
	ChunkID      uuid.UUID
	Version      int
	ChunkServers []uuid.UUID
	Lease        uuid.UUID
	Checksum     int
	Index        int
}
