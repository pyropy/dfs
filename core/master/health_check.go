package master

import (
	"github.com/google/uuid"
	chunkServerRPC "github.com/pyropy/dfs/rpc/chunkserver"
	"log"
	"net/rpc"
)

func HealthCheck(chunkServer ChunkServerMetadata, unhealthy chan uuid.UUID) {
	client, err := rpc.DialHTTP("tcp", chunkServer.Address)
	if err != nil {
		log.Println("error", chunkServer.Address, "unreachable")
		unhealthy <- chunkServer.ID
		return
	}

	defer client.Close()

	var reply chunkServerRPC.HealthCheckReply
	args := &chunkServerRPC.HealthCheckArgs{}
	err = client.Call("ChunkServerAPI.HealthCheck", args, &reply)
	if err != nil {
		log.Println("error", chunkServer.Address, "error", err)
		unhealthy <- chunkServer.ID
		return
	}

	if reply.Status < 299 {
		log.Println("info", chunkServer.ID, "server is healthy")
		return
	}

	unhealthy <- chunkServer.ID
}
