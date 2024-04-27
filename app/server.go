package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/info"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/services"
	"github.com/google/uuid"
)

func main() {
	s := NewServer()
	s.Start()
}

const (
	// Server options
	PORT       = "--port"
	REPLICA_OF = "--replicaof"
)

type Server struct {
	QChan      chan any
	connChan   chan net.Conn
	serverInfo info.ServerInfo
	rs         *services.RedisService
	metrics    *info.Metrics
}

type serverOptions struct {
	port       int
	role       string
	masterHost string
	masterPort int
}

func NewServer() *Server {
	return &Server{
		connChan:   make(chan net.Conn),
		rs:         services.NewRedisService(),
		serverInfo: make(info.ServerInfo),
		metrics:    info.NewMetrics(),
	}
}

func (s *Server) Start() {

	serverOps := buildServerOptions()
	s.serverInfo[info.SERVER_ROLE] = serverOps.role
	s.serverInfo[info.SERVER_PORT] = strconv.Itoa(serverOps.port)
	s.serverInfo[info.SERVER_MASTER_REPLID] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

	if s.serverInfo[info.SERVER_ROLE] == info.ROLE_SLAVE {
		portString := strconv.Itoa(serverOps.masterPort)
		s.serverInfo[info.SERVER_MASTER_HOST] = serverOps.masterHost
		s.serverInfo[info.SERVER_MASTER_PORT] = portString

		masterConn, err := net.Dial("tcp", serverOps.masterHost+":"+portString)

		if err != nil {
			log.Fatal("could not connect to master, info:\n", s.serverInfo)
		}

		go replication.HandleSlaveConn(masterConn, s.buildCtx())
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

			if processedArgs+2 >= len(args) {
				log.Fatal("missing replica of arguments")
			}
			processedArgs++
			masterHost := args[processedArgs]
			processedArgs++
			masterPort, err := strconv.Atoi(args[processedArgs])
			if err != nil {
				log.Fatal("Could not get master port value:" + err.Error())
			}

			ops.masterHost = masterHost
			ops.masterPort = masterPort
			ops.role = info.ROLE_SLAVE

		default:
			processedArgs++
		}

	}
	return ops
}
