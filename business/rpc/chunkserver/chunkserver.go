package chunkserver

import "github.com/google/uuid"

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

type ChunkServer interface {
	HealthCheck(args *HealthCheckArgs, reply *HealthCheckReply) error
	CreateChunk(args *CreateChunkRequest, reply *CreateChunkReply) error
}
