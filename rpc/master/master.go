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
	// ReportHealth ...
	ReportHealth(args ReportHealthArgs, reply ReportHealthReply) error
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

type ChunkServer struct {
	ID      uuid.UUID
	Address string
}

type RequestWriteReply struct {
	ChunkID              uuid.UUID
	Version              int
	PrimaryChunkServerID uuid.UUID
	ValidUntil           time.Time
	ChunkServers         []ChunkServer
}

type Chunk struct {
	ID      uuid.UUID
	Version int
	Index   int
}

type ReportHealthArgs struct {
	ChunkServerID uuid.UUID
	Chunks        []Chunk
}

type ReportHealthReply struct {
}
