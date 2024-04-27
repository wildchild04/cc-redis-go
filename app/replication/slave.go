package replication

import (
	"bufio"
	"context"
	"io"
	"log"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/protocol/parser"
	respencoding "github.com/codecrafters-io/redis-starter-go/app/protocol/resp_encoding"
)

func HandleSlaveConn(conn net.Conn, ctx context.Context) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	parser := parser.NewParser(reader)
	conn.Write(respencoding.EncodeArray([][]byte{[]byte("ping")}))

	cmd, err := parser.GetCmdInfo()

	if err != nil {
		if err == io.EOF {

			return
		}

		log.Println("Error getting cmd from master", err)
	}

	log.Printf("reply from master %+v", cmd)

}
