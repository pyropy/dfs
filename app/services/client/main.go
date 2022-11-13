package main

import (
	"bytes"
	"crypto/rand"
	"log"
	"time"

	"github.com/pyropy/dfs/business/core/client"
	filemetadataservice "github.com/pyropy/dfs/business/core/file_metadata_service"
)

func main() {
	path := "/test/me"
	c, err := client.NewClient("localhost:1234")
	if err != nil {
		log.Println("error", "unreachable")
		log.Fatalln(err)
		return
	}

	newFileReply, err := c.CreateNewFile(path, 64*10e+6)
	if err != nil {
		log.Fatalln(err)
		return
	}

	metadata := filemetadataservice.NewFileMetadata(path)
	metadata.Chunks = newFileReply.Chunks
	c.AddNewFileMetadata(path, metadata)

	log.Println("Create new file", newFileReply)

	// for i := 0; i < 5; i++ {
	b := make([]byte, 1024)
	rand.Read(b)

	buff := bytes.NewBuffer(b)

	bw, err := c.WriteFile(path, buff, 0)
	if err != nil {
		log.Fatalln(err)
		return
	}

	log.Println("Bytes written", bw)
	time.Sleep(time.Second * 1)
	// }

}
