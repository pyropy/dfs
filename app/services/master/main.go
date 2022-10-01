package main

import (
	"github.com/pyropy/dfs/business/core"
	masterRPC "github.com/pyropy/dfs/business/rpc/master"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
)

var (
	ReplicationFactor     = 3
	chunkSizeBytes    int = 64 * 10e+6
)

type MasterAPI struct {
	Master *core.Master
}

func NewMasterAPI(master *core.Master) *MasterAPI {
	return &MasterAPI{
		Master: master,
	}
}

func (m *MasterAPI) RegisterChunkServer(args *masterRPC.RegisterArgs, reply *masterRPC.RegisterReply) error {
	m.Master.Mutex.Lock()
	defer m.Master.Mutex.Unlock()

	chunkServer := m.Master.RegisterNewChunkServer(args.Address)
	reply.ID = chunkServer.ID

	log.Println("Registered new chunk server", chunkServer.ID, chunkServer.Address)

	return nil
}

func (m *MasterAPI) CreateNewFile(args *masterRPC.CreateNewFileArgs, reply *masterRPC.CreateNewFileReply) error {

	file, err := m.Master.CreateNewFile(args.Path, args.Size, ReplicationFactor, chunkSizeBytes)
	if err != nil {
		return err
	}

	reply.Chunks = file.Chunks
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalln("startup", "ERROR", err)
	}
}

func run() error {
	master := core.NewMaster()
	masterAPI := NewMasterAPI(master)

	rpc.Register(masterAPI)
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
	go master.StartHealthCheckService()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
	log.Println("shutdown", "status", "master rpc server stopping", l.Addr().String())

	return nil
}
