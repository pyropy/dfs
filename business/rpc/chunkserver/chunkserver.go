package chunkserver

import (
	"github.com/google/uuid"
	"time"
)

type HealthCheckArgs struct {
}

type HealthCheckReply struct {
	// TODO: Add chunk info to reply
	Status int
}

type CreateChunkRequest struct {
	ChunkID      uuid.UUID
	ChunkVersion int
	ChunkSize    int
}

type CreateChunkReply struct {
	ChunkID uuid.UUID
}

type GrantLeaseArgs struct {
	ChunkID    uuid.UUID
	ValidUntil time.Time
}

type GrantLeaseReply struct {
}

type IncrementChunkVersionArgs struct {
	Version int // used to validate
	ChunkID uuid.UUID
}

type IncrementChunkVersionReply struct {
}

type ChunkServer interface {
	HealthCheck(args *HealthCheckArgs, reply *HealthCheckReply) error
	CreateChunk(args *CreateChunkRequest, reply *CreateChunkReply) error
	GrantLease(args *GrantLeaseArgs, reply *GrantLeaseReply) error
	IncrementChunkVersion(args *IncrementChunkVersionArgs, reply *IncrementChunkVersionReply) error
}
