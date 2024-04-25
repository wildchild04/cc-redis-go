package services

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/protocol/parser"
)

const (
	PING = "ping"
	ECHO = "echo"
)

func HandleConn(conn net.Conn) {

	defer log.Println("clossing conn:", conn.RemoteAddr())
	defer conn.Close()
	for {
		reader := bufio.NewReader(conn)
		reader.Peek(2)
		p := parser.NewParser(reader)

		cmd, err := p.GetCmdInfo()

		if err != nil {
			if err == io.EOF {
				return
			}
			log.Println("Error getting cmd: ", err)
			return
		}

		resp := getCmdResponse(cmd)
		log.Printf("response to %s : %s", cmd, resp)
		conn.Write(resp)
	}

}

func getCmdResponse(cmdInfo parser.CmdInfo) []byte {

	switch cmdInfo.CmdName {

	case PING:
		return addCRNL("+PONG")
	case ECHO:
		return addCRNL("+" + cmdInfo.Args[0])
	}

	return []byte("+\r\n")
}

func addCRNL(s string) []byte {
	return []byte(fmt.Sprintf("%s\r\n", s))
}
