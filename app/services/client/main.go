package main

import (
	"log"

	"github.com/pyropy/dfs/business/core/client"
)

func main() {
	c, err := client.NewClient("localhost:1234")
	if err != nil {
		log.Println("error", "unreachable")
		log.Fatalln(err)
		return
	}

	newFileReply, err := c.CreateNewFile("/test/me", 64*10e+6)
	if err != nil {
		log.Fatalln(err)
		return
	}

	log.Println("Create new file", newFileReply)

	firstChunkID := newFileReply.Chunks[0]
	reqWriteReply, err := c.RequestChunkWrite(firstChunkID)
	if err != nil {
		log.Fatalln(err)
		return
	}

	log.Println(reqWriteReply)

}
