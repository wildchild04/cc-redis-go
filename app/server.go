package main

import (
	"log"
	"net"
	"os"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/services"
)

func main() {
	server := NewServer()
	server.Start()
}

const (
	PORT = "--port"
)

type Server struct {
	QChan    chan any
	connChan chan net.Conn
	rs       *services.RedisService
}

type serverOptions struct {
	port int
}

func NewServer() *Server {
	return &Server{connChan: make(chan net.Conn), rs: services.NewRedisService()}
}

func (s *Server) Start() {

	serverOps := buildServerOptions()
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
	go handleConn(s.connChan, s.rs)
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

func handleConn(connChan chan net.Conn, rs *services.RedisService) {

	for {
		select {
		case conn := <-connChan:
			go rs.HandleConn(conn)
		}
	}
}

func buildServerOptions() serverOptions {
	args := os.Args
	ops := serverOptions{}

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
