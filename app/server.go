package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/info"
	"github.com/codecrafters-io/redis-starter-go/app/services"
	"github.com/google/uuid"
)

func main() {
	s := NewServer()
	s.Start()
}

const (
	// Server options
	PORT = "--port"
)

type Server struct {
	QChan      chan any
	connChan   chan net.Conn
	serverInfo info.ServerInfo
	rs         *services.RedisService
}

type serverOptions struct {
	port int
	role string
}

func NewServer() *Server {
	return &Server{connChan: make(chan net.Conn), rs: services.NewRedisService(), serverInfo: make(info.ServerInfo)}
}

func (s *Server) Start() {

	serverOps := buildServerOptions()
	s.serverInfo[info.SERVER_ROLE] = serverOps.role
	s.serverInfo[info.SERVER_PORT] = strconv.Itoa(serverOps.port)

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
	ops.role = "master"

	processedArgs := 0
	for processedArgs < len(args) {
		switch args[processedArgs] {
		case PORT:
			if processedArgs+1 >= len(args) {
				log.Fatal("missing port argutmen")
			}
			processedArgs++
			port, err := strconv.Atoi(args[processedArgs])
			if err != nil {
				log.Fatal("Could not get the port value: " + err.Error())
			}

			ops.port = port
		default:
			processedArgs++
		}

	}
	return ops
}
