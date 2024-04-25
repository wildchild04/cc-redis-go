package services

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/protocol/parser"
)

const (
	//CMD names
	PING = "ping"
	ECHO = "echo"
	SET  = "set"
	GET  = "get"

	//RESP3 reply
	NULLS     = "_\r\n"
	NULL_BULK = "$-1\r\n"

	//SET OPTIONS
	PX = "px"
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
		if len(cmdInfo.Args) < 2 {
			return encodeSimpleError("Not enough args for SET: " + strings.Join(cmdInfo.Args, ","))
		}
		ok := false

		key := cmdInfo.Args[0]
		val := cmdInfo.Args[1]
		if len(cmdInfo.Args) > 2 {

			ops, err := buildKvsOptions(cmdInfo.Args[2:])
			if err != nil {
				return encodeSimpleError(err.Error())
			}
			ok = rs.kvs.SetWithOptions(key, []byte(val), ops)
		} else {

			ok = rs.kvs.Set(key, []byte(val))
		}
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
			return []byte(NULL_BULK)
		}
	}

	return encodeSimpleString("UNKNOWN CMD")
}

func encodeBulkArray(s ...[]byte) []byte {

	buffer := make([]byte, 0, 1024)
	for i := range s {
		buffer = append(buffer, '$')
		size := strconv.Itoa(len(s[i]))
		buffer = append(buffer, size...)
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

func buildKvsOptions(args []string) (KvsOptions, error) {

	ops := KvsOptions{}
	processedLines := 0
	for processedLines < len(args) {

		switch args[processedLines] {
		case PX:
			if processedLines+1 >= len(args) {
				return KvsOptions{}, fmt.Errorf("Missing PX value")
			}
			processedLines++
			pxArg := args[processedLines]
			pxNumericVal, err := strconv.Atoi(pxArg)
			if err != nil {
				return KvsOptions{}, fmt.Errorf("PX value is not a number: %s", pxArg)
			}
			ops.expires = time.Duration(pxNumericVal)

			processedLines++
		default:
			processedLines++
		}

	}

	return ops, nil
}
