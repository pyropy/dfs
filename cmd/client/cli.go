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

// TODO: Separate create and write Command
// Write command should take stream of bytes and write them at given offset
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
	Action: func(cctx *cli.Context) error {
		filePath := cctx.String("file-path")
		dfsPath := cctx.String("dfs-path")
		storePath := cctx.String("store")
		rpcUrl := cctx.String("rpc-url")

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

		ctx := context.Background()

		metadata := model.NewFileMetadata(dfsPath)
		metadata.Chunks = newFileReply.Chunks
		err = c.AddNewFileMetadata(ctx, dfsPath, metadata)
		if err != nil {
			return err
		}

		log.Infow("Created new file", "chunks", newFileReply.Chunks, "chunkServers", newFileReply.ChunkServerIDs)

		content, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}

		log.Debugw("read contents", "filePath", filePath, "size", len(content))

		buff := bytes.NewBuffer(content)

		bw, err := c.WriteFile(ctx, dfsPath, buff, 0)
		if err != nil {
			return err
		}

		log.Infow("Bytes written", "bytes", bw, "offset", 0)
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


// TODO: Add delete command
