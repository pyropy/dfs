package main

import (
	"log"
	"net/rpc"

	"github.com/pyropy/dfs/business/rpc/master"
)

func main() {
	client, err := rpc.DialHTTP("tcp", "localhost:1234")
	if err != nil {
		log.Println("error", "unreachable")
		log.Fatalln(err)
		return
	}

	var reply master.CreateNewFileReply
	args := &master.CreateNewFileArgs{
		Path: "/test/me",
		Size: 1 * 10e+6,
	}

	err = client.Call("MasterAPI.CreateNewFile", args, &reply)
	if err != nil {
		log.Fatalln(err)
		return
	}
}
