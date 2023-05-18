package main

import (
    "os"
    "github.com/urfave/cli/v2"
	"github.com/pyropy/dfs/lib/logger"
)

var log, _ = logger.New("client")

func main() {
    local := []*cli.Command{
        writeCmd,
        listCmd,
    }

	app := &cli.App{
		Name:     "dfs-client",
		Usage:    "Client",
		Version:  "0.0.1",
		Commands: local,
		Flags: []cli.Flag{
            &cli.StringFlag{
                Name: "rpc-url",
                Value: "localhost:1234",
                Usage: "Master rpc address",
            },
            &cli.StringFlag{
                Name: "store",
                Value: ".client",
                Usage: "Path where chunk metadata is persisted at",
            },
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
		return
	}
}
