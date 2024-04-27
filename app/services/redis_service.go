package services

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/info"
	"github.com/codecrafters-io/redis-starter-go/app/protocol/parser"
	respencoding "github.com/codecrafters-io/redis-starter-go/app/protocol/resp_encoding"
)

const (
	//CMD names
	PING     = "ping"
	ECHO     = "echo"
	SET      = "set"
	GET      = "get"
	INFO     = "info"
	REPLCONF = "replconf"

	//RESP3 reply
	NULLS     = "_\r\n"
	NULL_BULK = "$-1\r\n"

	//SET OPTIONS
	PX = "px"

	//info cmd
	REPLICATION = "#Replication"
)

type RedisService struct {
	kvs Kvs
}

func NewRedisService() *RedisService {
	return &RedisService{NewKvSService()}
}

func (rs *RedisService) HandleConn(conn net.Conn, ctx context.Context) {

	defer log.Println("clossing conn:", conn.RemoteAddr())
	defer conn.Close()
	for {
		reader := bufio.NewReader(conn)
		reader.Peek(2)
		p := parser.NewParser(reader)

		incoming, err := p.ParseIncomingData()
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Println("Error getting cmd: ", err)
			return
		}

		switch incoming.(type) {
		case parser.CmdInfo:
			cmd := incoming.(parser.CmdInfo)
			resp := rs.getCmdResponse(&cmd, ctx)
			log.Printf("response to %+v:\n%s\n", cmd, resp)
			conn.Write(resp)
		case parser.SimpleString:
			log.Println("got simple string\n", incoming)
		case parser.UnknownData:
			log.Printf("got unknown data,\ndata type: '%c' \ndata: '%s'",
				incoming.(parser.UnknownData).Dt, string(incoming.(parser.UnknownData).Data))
		}
	}
}

func (rs *RedisService) getCmdResponse(cmdInfo *parser.CmdInfo, ctx context.Context) []byte {

	switch cmdInfo.CmdName {
	case PING:
		return respencoding.EncodeSimpleString("PONG")
	case ECHO:
		return respencoding.EncodeSimpleString(cmdInfo.Args[0])
	case SET:
		if len(cmdInfo.Args) < 2 {
			return respencoding.EncodeSimpleError("Not enough args for SET: " + strings.Join(cmdInfo.Args, ","))
		}
		ok := false

		key := cmdInfo.Args[0]
		val := cmdInfo.Args[1]
		if len(cmdInfo.Args) > 2 {

			ops, err := buildKvsOptions(cmdInfo.Args[2:])
			if err != nil {
				return respencoding.EncodeSimpleError(err.Error())
			}
			ok = rs.kvs.SetWithOptions(key, []byte(val), ops)
		} else {

			ok = rs.kvs.Set(key, []byte(val))
		}
		if ok {
			return respencoding.EncodeSimpleString("OK")
		} else {
			return respencoding.EncodeSimpleError("ERR: could not store k/v")
		}
	case GET:
		value, ok := rs.kvs.Get(cmdInfo.Args[0])
		if ok {
			return respencoding.EncodeBulkStringArray([][]byte{value})
		} else {
			return []byte(NULL_BULK)
		}
	case INFO:
		if len(cmdInfo.Args) < 1 {
			return respencoding.EncodeBulckString(info.BuildInfo("", ctx))
		} else {
			return respencoding.EncodeBulckString(info.BuildInfo(cmdInfo.Args[0], ctx))
		}
	case REPLCONF:
		log.Println("Replication config received", cmdInfo)
		return respencoding.EncodeSimpleString("OK")
	}

	return respencoding.EncodeSimpleString("UNKNOWN CMD")
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
