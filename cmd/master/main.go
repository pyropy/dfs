package main

import (
	"github.com/pyropy/dfs/core/constants"
	master2 "github.com/pyropy/dfs/core/master"
	masterRPC "github.com/pyropy/dfs/rpc/master"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
)

type MasterAPI struct {
	Master *master2.Master
}

func NewMasterAPI(master *master2.Master) *MasterAPI {
	return &MasterAPI{
		Master: master,
	}
}

func (m *MasterAPI) RegisterChunkServer(args *masterRPC.RegisterArgs, reply *masterRPC.RegisterReply) error {
	chunkServer := m.Master.RegisterNewChunkServer(args.Address)
	reply.ID = chunkServer.ID

	log.Println("Registered new chunk server", chunkServer.ID, chunkServer.Address)

	return nil
}

func (m *MasterAPI) CreateNewFile(args *masterRPC.CreateNewFileArgs, reply *masterRPC.CreateNewFileReply) error {
	log.Println("CreateNewFile", args)
	file, chunkServerIds, err := m.Master.CreateNewFile(args.Path, args.Size, constants.REPLICATION_FACTOR, constants.CHUNK_SIZE_BYTES)
	if err != nil {
		return err
	}

	reply.Chunks = file.Chunks
	reply.ChunkServerIDs = chunkServerIds
	return nil
}

func (m *MasterAPI) RequestLeaseRenewal(args *masterRPC.RequestLeaseRenewalArgs, reply *masterRPC.RequestLeaseRenewalReply) error {
	log.Println("RequestLeaseRenewal", args)
	chs := master2.ChunkServerMetadata{
		ID: args.ChunkServerID,
	}

	lease, err := m.Master.RequestLeaseRenewal(args.ChunkID, &chs)
	if err != nil {
		return err
	}

	reply.Granted = true
	reply.ChunkID = lease.ChunkID
	reply.ValidUntil = lease.ValidUntil
	return nil
}

func (m *MasterAPI) RequestWrite(args *masterRPC.RequestWriteArgs, reply *masterRPC.RequestWriteReply) error {
	log.Println("RequestWrite", args)
	chunkServers := []masterRPC.ChunkServer{}
	lease, chunkVersion, err := m.Master.RequestWrite(args.ChunkID)
	if err != nil {
		return err
	}

	chunkHoldersIDs := m.Master.GetChunkHolders(args.ChunkID)
	for _, chunkHolderID := range chunkHoldersIDs {
		chunkHolder := m.Master.GetChunkServerMetadata(chunkHolderID)
		chunkServer := masterRPC.ChunkServer{
			ID:      chunkHolder.ID,
			Address: chunkHolder.Address,
		}
		chunkServers = append(chunkServers, chunkServer)
	}

	reply.ChunkID = args.ChunkID
	reply.PrimaryChunkServerID = lease.ChunkServerID
	reply.ValidUntil = lease.ValidUntil
	reply.ChunkServers = chunkServers
	reply.Version = chunkVersion

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalln("startup", "ERROR", err)
	}
}

func run() error {
	master := master2.NewMaster()
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
