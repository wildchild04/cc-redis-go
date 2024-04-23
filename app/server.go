package main

import (
	"fmt"
	"net"
	"os"
)

func main() {

	server := NewServer()
	server.start()

	<-server.QChan
}

type Server struct {
	QChan chan any
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) start() {

	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	go func(conn net.Conn) {

		buffer := make([]byte, 1024)
		readCount, err := conn.Read(buffer)

		if err != nil {
			fmt.Println("error reading incoming data", err)
		}

		message := string(buffer[:readCount:readCount])
		fmt.Printf("message '%s'", message)

		if message == "*1\r\n$4\r\nping\r\n" {
			conn.Write([]byte("+PONG\r\n"))
		}

		conn.Close()

	}(conn)
}
