package chunkserver

import (
	"github.com/google/uuid"
	"time"
)

type CreateChunkRequest struct {
	ChunkID      uuid.UUID
	ChunkVersion int
	ChunkSize    int
	ChunkIndex   int
	FilePath     string
}

type CreateChunkReply struct {
	ChunkID      uuid.UUID
	ChunkVersion int
	ChunkIndex   int
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

type TransferDataArgs struct {
	CheckSum int
	Data     []byte
}

type TransferDataReply struct {
	NumBytesReceived int
}

type ChunkServer struct {
	ID      uuid.UUID
	Address string
}

type WriteChunkArgs struct {
	ChunkID  uuid.UUID
	CheckSum int
	Offset   int
	Version  int

	ChunkServers []ChunkServer
}

type WriteChunkReply struct {
	BytesWritten int
}

type ApplyMigrationArgs struct {
	ChunkID  uuid.UUID
	CheckSum int
	Offset   int
	Version  int
}

type ApplyMigrationReply struct {
	BytesWritten int
}

type ReplicateChunkArgs struct {
	ChunkID      uuid.UUID
	ChunkServers []ChunkServer
}

type ReplicateChunkReply struct {
}

type DeleteChunkRequest struct {
	ChunkID uuid.UUID
}

type DeleteChunkReply struct {
}

type IChunkServer interface {
	CreateChunk(args *CreateChunkRequest, reply *CreateChunkReply) error
	DeleteChunk(args *DeleteChunkRequest, reply *DeleteChunkReply) error
	GrantLease(args *GrantLeaseArgs, reply *GrantLeaseReply) error
	IncrementChunkVersion(args *IncrementChunkVersionArgs, reply *IncrementChunkVersionReply) error
	TransferData(args *TransferDataArgs, reply *TransferDataReply) error
	WriteChunk(args *WriteChunkArgs, reply *WriteChunkReply) error
	ApplyMigration(args *ApplyMigrationArgs, reply *ApplyMigrationReply) error
	ReplicateChunk(args *ReplicateChunkArgs, reply *ReplicateChunkReply) error
}
