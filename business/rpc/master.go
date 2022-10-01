package rpc

type Master interface {
	RegisterChunkServer(args RegisterArgs, reply RegisterReply) error
}

type RegisterArgs struct {
	Address string
}

type RegisterReply struct {
	ID int
}
