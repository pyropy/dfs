package master

import (
	"github.com/google/uuid"
)

type Master interface {
	// RegisterChunkServer ...
	RegisterChunkServer(args RegisterArgs, reply RegisterReply) error
	// CreateNewFile ...
	CreateNewFile(args CreateNewFileArgs, reply CreateNewFileReply) error
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
	Chunks []uuid.UUID
}
