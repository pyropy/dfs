package main

import (
	"github.com/pyropy/dfs/business/core"
	rpcCore "github.com/pyropy/dfs/business/rpc"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type MasterAPI struct {
	Master *core.Master
}

func (m *MasterAPI) RegisterChunkServer(args rpcCore.RegisterArgs, reply rpcCore.RegisterReply) error {
	m.Master.Mutex.Lock()
	defer m.Master.Mutex.Unlock()

	chunkServer := m.Master.RegisterChunkServer(args.Address)
	reply.ID = chunkServer.ID

	return nil
}

// TODO: Move to core
func CheckHealth(master *core.Master) {
	ticker := time.NewTicker(1 * time.Second)

	checkHealth := func(i int) {
		chunkServer := master.Chunkservers[i]
		client, err := rpc.DialHTTP("tcp", chunkServer.Address)
		if err != nil {
			log.Println("error", chunkServer.Address, "unreachable")
			master.Chunkservers[i].Healthy = false
			return
		}

		var reply rpcCore.HealthCheckReply
		args := &rpcCore.HealthCheckArgs{}
		err = client.Call("ChunkServerAPI.HealthCheck", args, &reply)
		if err != nil {
			log.Println("error", chunkServer.Address, "error", err)
			master.Chunkservers[i].Healthy = false
			return
		}

		if reply.Status < 299 {
			master.Chunkservers[i].Healthy = true
			return
		}

		master.Chunkservers[i].Healthy = false
	}

	for {
		select {
		case _ = <-ticker.C:
			for i := 0; i < len(master.Chunkservers); i++ {
				go checkHealth(i)
			}
		}
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatalln("startup", "ERROR", err)
	}
}

func run() error {
	master := new(core.Master)
	masterAPI := MasterAPI{
		Master: master,
	}
	rpc.Register(&masterAPI)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Println("startup", "error", "net listen failed")
		return err
	}

	log.Println("startup", "status", "master rpc server started", l.Addr().String())
	defer log.Println("shutdown", "status", "master rpc server stopped", l.Addr().String())
	go http.Serve(l, nil)

	log.Println("startup", "status", "chunkservers found", len(master.Chunkservers))

	log.Println("startup", "status", "starting healtcheck")
	go CheckHealth(master)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
	log.Println("shutdown", "status", "master rpc server stopping", l.Addr().String())

	return nil
}
