package main

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/pyropy/dfs/core/client"
	"github.com/pyropy/dfs/core/model"
	"github.com/urfave/cli/v2"
)

var writeCmd = &cli.Command{
	Name: "write",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "file-path",
			Required: true,
			Usage:    "Path to file you want to write to dfs",
		},
		&cli.StringFlag{
			Name:     "dfs-path",
			Required: true,
			Usage:    "Path where you want to store your file on dfs",
		},
	},
	Action: func(ctx *cli.Context) error {
		filePath := ctx.String("file-path")
		dfsPath := ctx.String("dfs-path")
		storePath := ctx.String("store")
		rpcUrl := ctx.String("rpc-url")

		c, err := client.NewClient(rpcUrl, storePath)
		if err != nil {
			return err
		}

		fi, err := os.Stat(filePath)
		if err != nil {
			return err
		}

		newFileReply, err := c.CreateNewFile(dfsPath, int(fi.Size()))
		if err != nil {
			return err
		}

		cctx := context.Background()

		metadata := model.NewFileMetadata(dfsPath)
		metadata.Chunks = newFileReply.Chunks
		err = c.AddNewFileMetadata(cctx, dfsPath, metadata)
		if err != nil {
			return err
		}

		log.Info("Created new file ", " chunks ", newFileReply.Chunks, " chunk servers ", newFileReply.ChunkServerIDs)

		content, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}

		buff := bytes.NewBuffer(content)

		bw, err := c.WriteFile(dfsPath, buff, 0)
		if err != nil {
			log.Fatalln(err)
			return err
		}

		log.Info("Bytes written", bw, "at offset", 0)
		return nil
	},
}

var listCmd = &cli.Command{
	Name:  "list",
	Usage: "List all files",
	Action: func(ctx *cli.Context) error {
		storePath := ctx.String("store")
		rpcUrl := ctx.String("rpc-url")

		c, err := client.NewClient(rpcUrl, storePath)
		if err != nil {
			return err
		}

		cctx := context.Background()

		files, err := c.FileMetadataStore.All(cctx)
		if err != nil {
			return err
		}

		for _, file := range files {
			fmt.Println(file)
		}

		return nil
	},
}
