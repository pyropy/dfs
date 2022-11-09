package master

import (
	"time"

	"github.com/google/uuid"
)

type Master interface {
	// RegisterChunkServer ...
	RegisterChunkServer(args RegisterArgs, reply RegisterReply) error
	// CreateNewFile ...
	CreateNewFile(args CreateNewFileArgs, reply CreateNewFileReply) error
	// RequestLeaseRenewal ...
	RequestLeaseRenewal(args RequestLeaseRenewalArgs, reply RequestLeaseRenewalReply) error
	// RequestWrite ...
	RequestWrite(args RequestWriteArgs, reply RequestWriteReply) error
}

type RegisterArgs struct {
	Address string
}

type RegisterReply struct {
	ID uuid.UUID
}

type CreateNewFileArgs struct {
	Path string
	Size int
}

type CreateNewFileReply struct {
	Chunks         []uuid.UUID
	ChunkServerIDs []uuid.UUID
}

type RequestLeaseRenewalArgs struct {
	ChunkID       uuid.UUID
	ChunkServerID uuid.UUID
}

type RequestLeaseRenewalReply struct {
	Granted    bool
	ChunkID    uuid.UUID
	ValidUntil time.Time
}

type RequestWriteArgs struct {
	ChunkID uuid.UUID
}

type RequestWriteReply struct {
	ChunkID       uuid.UUID
	ChunkServerID uuid.UUID
	ValidUntil    time.Time
}
