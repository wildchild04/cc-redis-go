package main

import (
	"bufio"
	"log"
	"net"
	"os"
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
			go redisService(conn)
		}
	}
}

func redisService(conn net.Conn) error {

	for {
		messageBuffer := make([]byte, 0, 1024)

		reader := bufio.NewReader(conn)
		reads := 0

		for reads < 3 {

			readLine, err := reader.ReadSlice('\n')
			if err != nil {
				return err
			}
			messageBuffer = append(messageBuffer, readLine...)
			reads++
		}

		message := string(messageBuffer)
		log.Printf("message \n'''\n%s'''\n", message)

		if message == "*1\r\n$4\r\nping\r\n" {
			log.Println("Pong reply")
			conn.Write([]byte("+PONG\r\n"))
		} else {
			conn.Write([]byte("+\r\n"))
		}

	}

}
