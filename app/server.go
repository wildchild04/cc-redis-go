package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/info"
	"github.com/codecrafters-io/redis-starter-go/app/protocol/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/services"
	"github.com/google/uuid"
)

func main() {
	s := NewServer()
	s.Start()
}

const (
	// Server options
	PORT        = "--port"
	REPLICA_OF  = "--replicaof"
	DIR         = "--dir"
	DB_FILENAME = "--dbfilename"
)

type Server struct {
	QChan              chan any
	connChan           chan net.Conn
	serverInfo         info.ServerInfo
	kvs                services.Kvs
	rs                 *services.RedisService
	masterService      services.MasterService
	replicationService services.ReplicationService
	metrics            *info.Metrics
}

type serverOptions struct {
	port        int
	role        string
	masterHost  string
	masterPort  int
	rbdDir      string
	rbdFileName string
}

func NewServer() *Server {
	kvs := services.NewKvSService()
	return &Server{
		kvs:        kvs,
		connChan:   make(chan net.Conn),
		rs:         services.NewRedisService(kvs),
		serverInfo: make(info.ServerInfo),
		metrics:    info.NewMetrics(),
	}
}

func (s *Server) Start() {

	serverOps := buildServerOptions()
	s.serverInfo[info.SERVER_ROLE] = serverOps.role
	s.serverInfo[info.SERVER_PORT] = strconv.Itoa(serverOps.port)
	s.serverInfo[info.SERVER_MASTER_REPLID] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	if serverOps.rbdDir != "" && serverOps.rbdFileName != "" {
		s.serverInfo[info.SERVER_RDB_DIR] = serverOps.rbdDir

		file, err := os.Open(fmt.Sprintf("%s/%s", serverOps.rbdDir, serverOps.rbdFileName))
		if err != nil {
			log.Printf("Error reading file at %s, error: %s", serverOps.rbdDir, err)
		} else {

			info, _ := file.Stat()

			rdbBytes := rdb.BuildRDBFromFileSystem(file, info.Size())
			rdbFile, err := rdb.LoadRDBFile(rdbBytes)

			if err != nil {
				log.Fatalf("Error loading rdb file %s", err)
			}

			for _, simplePair := range rdbFile.Kv {
				s.kvs.Set(simplePair.Key, []byte(simplePair.Value))
			}
		}

	}

	if s.serverInfo[info.SERVER_ROLE] == info.ROLE_SLAVE {
		s.replicationService = services.NewReplicationService(s.kvs, s.metrics)
		portString := strconv.Itoa(serverOps.masterPort)
		s.serverInfo[info.SERVER_MASTER_HOST] = serverOps.masterHost
		s.serverInfo[info.SERVER_MASTER_PORT] = portString

		masterConn, err := net.Dial("tcp", serverOps.masterHost+":"+portString)

		if err != nil {
			log.Fatal("could not connect to master, info:\n", s.serverInfo)
		}

		go s.replicationService.HandleMasterConn(masterConn, s.buildCtx())
	}

	if s.serverInfo[info.SERVER_ROLE] == info.ROLE_MASTER {

		s.masterService = services.NewMasterService(s.metrics, s.connChan)
		go s.masterService.HandleEvents()
	}

	log.Println("Starting server\n INFO", s.serverInfo)
	var listener net.Listener
	var err error

	if serverOps.port != 0 {
		listener, err = s.createConnection(serverOps.port)
	} else {
		listener, err = s.createDefaultConnection()
	}

	if err != nil {
		log.Fatal("Could not create connection" + err.Error())
	}

	defer listener.Close()
	go s.handleConn(s.connChan, s.rs)
	go listen(listener, s)

	<-s.QChan
}

func (s *Server) createDefaultConnection() (net.Listener, error) {
	return s.createConnection(6379)
}

func (s *Server) createConnection(port int) (net.Listener, error) {

	portStr := strconv.Itoa(port)

	log.Println("Accepting connections using port:" + portStr)

	l, err := net.Listen("tcp", "0.0.0.0:"+portStr)

	if err != nil {
		log.Printf("Failed to bind to port %s, err: %s ", portStr, err)
		os.Exit(1)
	}

	return l, nil
}

func (s *Server) handleConn(connChan chan net.Conn, rs *services.RedisService) {

	for {
		select {
		case conn := <-connChan:
			go rs.HandleConn(conn, s.buildCtx())
		}
	}
}

func (s *Server) buildCtx() context.Context {
	ctx := context.Background()
	ctx = context.WithoutCancel(ctx)
	ctx = context.WithValue(ctx, info.CTX_SESSION_ID, uuid.New())
	ctx = context.WithValue(ctx, info.CTX_SERVER_INFO, s.serverInfo)
	ctx = context.WithValue(ctx, info.CTX_METRICS, s.metrics)
	if s.serverInfo[info.SERVER_ROLE] == info.ROLE_MASTER {
		ctx = context.WithValue(ctx, info.CTX_REPLICATION_EVENTS, s.masterService.GetReplicationCmdChan())
		ctx = context.WithValue(ctx, info.CTX_REPLACATION_REGISTRATION, s.masterService.GetReplicaRegistrationChan())
		ctx = context.WithValue(ctx, info.CTX_ACK_EVENT, s.masterService.GetAckEventChan())
		log.Println("Master ctx set", s.masterService.GetAckEventChan())
	}

	return ctx
}

func listen(l net.Listener, s *Server) {

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection", err)
		}

		log.Println("connection", conn.RemoteAddr())

		s.connChan <- conn
	}
}

func buildServerOptions() serverOptions {
	args := os.Args
	ops := serverOptions{}
	ops.port = 6379
	ops.role = info.ROLE_MASTER

	processedArgs := 0
	for processedArgs < len(args) {
		switch args[processedArgs] {
		case PORT:
			if processedArgs+1 >= len(args) {
				log.Fatal("missing port argument")
			}
			processedArgs++
			port, err := strconv.Atoi(args[processedArgs])
			if err != nil {
				log.Fatal("Could not get the port value:" + err.Error())
			}

			ops.port = port
		case REPLICA_OF:
			var replicaOfArgs []string
			if processedArgs+1 <= len(args) {
				replicaOfArgs = strings.Split(args[processedArgs+1], " ")
			}
			if processedArgs+2 >= len(args) && len(replicaOfArgs) <= 1 {
				log.Fatal("missing replica of arguments")
			}
			processedArgs++
			if len(replicaOfArgs) == 1 {
				replicaOfArgs = append(replicaOfArgs, args[processedArgs+1])
			}
			processedArgs++
			masterHost := replicaOfArgs[0]
			masterPort, err := strconv.Atoi(replicaOfArgs[1])
			if err != nil {
				log.Fatal("Could not get master port value:" + err.Error())
			}

			ops.masterHost = masterHost
			ops.masterPort = masterPort
			ops.role = info.ROLE_SLAVE
		case DIR:
			if processedArgs+1 >= len(args) {
				log.Fatal("missing dir argument")
			}
			processedArgs++
			dir := args[processedArgs]
			if dir == "" {
				log.Fatal("dir is empty")
			}
			ops.rbdDir = dir

		case DB_FILENAME:

			if processedArgs+1 >= len(args) {
				log.Fatal("missing dir name argument")
			}
			processedArgs++
			dirName := args[processedArgs]
			if dirName == "" {
				log.Fatal("dir name is empty")
			}
			ops.rbdFileName = dirName

		default:
			processedArgs++
		}

	}
	return ops
}
