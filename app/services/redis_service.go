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
	SET  = "set"
	GET  = "get"

	NULLS = "_\r\n"
)

type RedisService struct {
	kvs Kvs
}

func NewRedisService() *RedisService {
	return &RedisService{NewKvSService()}
}

func (rs *RedisService) HandleConn(conn net.Conn) {

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

		resp := rs.getCmdResponse(cmd)
		log.Printf("response to %s : %s", cmd, resp)
		conn.Write(resp)
	}

}

func (rs *RedisService) getCmdResponse(cmdInfo parser.CmdInfo) []byte {

	switch cmdInfo.CmdName {

	case PING:
		return encodeSimpleString("PONG")
	case ECHO:
		return encodeSimpleString(cmdInfo.Args[0])
	case SET:
		ok := rs.kvs.Set(cmdInfo.Args[0], []byte(cmdInfo.Args[1]))
		if ok {
			return encodeSimpleString("OK")
		} else {
			return encodeSimpleError("ERR: could not store k/v")
		}
	case GET:
		value, ok := rs.kvs.Get(cmdInfo.Args[0])
		if ok {
			return encodeBulkArray(value)
		} else {
			return []byte(NULLS)
		}
	}

	return encodeSimpleString("UNKNOWN CMD")
}

func encodeBulkArray(s ...[]byte) []byte {

	buffer := make([]byte, 0, 1024)
	for i := range s {
		buffer = append(buffer, '$')
		size := byte('0' + len(s[i]))
		buffer = append(buffer, size)
		buffer = append(buffer, parser.CRNL...)
		buffer = append(buffer, s[i]...)
		buffer = append(buffer, parser.CRNL...)
	}

	return buffer
}

func encodeSimpleString(s string) []byte {
	byteResp := make([]byte, 0, len(s)+3)
	byteResp = append(byteResp, '+')
	byteResp = append(byteResp, addCRNL(s)...)
	return byteResp
}

func encodeSimpleError(s string) []byte {

	byteResp := make([]byte, 0, len(s)+3)
	byteResp = append(byteResp, '-')
	byteResp = append(byteResp, addCRNL(s)...)
	return byteResp
}
func addCRNL(s string) []byte {
	return []byte(fmt.Sprintf("%s\r\n", s))
}
