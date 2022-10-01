package rpc

type HealthCheckArgs struct {
}

type HealthCheckReply struct {
	// TODO: Add chunk info to reply
	Status int
}

type CreateChunkRequest struct {
	ChunkID      int
	ChunkVersion int
}

type CreateChunkReply struct {
	ChunkID int
}

type ChunkServer interface {
	HealthCheck(args *HealthCheckArgs, reply *HealthCheckReply) error
	CreateChunk(args *CreateChunkRequest, reply *CreateChunkReply) error
}
