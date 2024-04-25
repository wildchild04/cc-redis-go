package main

import (
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/services"
)

func main() {

	server := NewServer()
	server.Start()

}

type Server struct {
	QChan    chan any
	connChan chan net.Conn
}

func NewServer() *Server {
	return &Server{connChan: make(chan net.Conn)}
}

func (s *Server) Start() {
	listener, err := s.createConnection()

	if err != nil {
		log.Fatal("Could not create connection" + err.Error())
	}

	defer listener.Close()
	go handleConn(s.connChan)
	go listen(listener, s)

	<-s.QChan

}

func (s *Server) createConnection() (net.Listener, error) {

	log.Println("Accepting connections")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		log.Println("Failed to bind to port 6379")
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

func handleConn(connChan chan net.Conn) {

	for {

		select {
		case conn := <-connChan:
			go services.HandleConn(conn)
		}
	}
}
