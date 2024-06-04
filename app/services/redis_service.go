package services

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/info"
	"github.com/codecrafters-io/redis-starter-go/app/protocol/parser"
	"github.com/codecrafters-io/redis-starter-go/app/protocol/rdb"
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
	PSYNC    = "psync"
	WAIT     = "wait"
	CONFIG   = "config"
	KEYS     = "keys"
	TYPE     = "type"
	XADD     = "xadd"
	XRANGE   = "xrange"
	XREAD    = "xread"

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

func NewRedisService(kvs Kvs) *RedisService {
	return &RedisService{kvs}
}

func (rs *RedisService) HandleConn(conn net.Conn, ctx context.Context) {

	shouldclose := true
OuterLoop:
	for {
		reader := bufio.NewReader(conn)
		p := parser.NewParser(reader)

		incoming, err := p.ParseIncomingData()
		if err != nil {
			if err == io.EOF {
				log.Println("done with client", conn.RemoteAddr())
				break
			}
			log.Println("Error getting cmd: ", err)
			break
		}

		switch incoming.(type) {
		case parser.CmdInfo:
			cmd := incoming.(parser.CmdInfo)
			if cmd.CmdName == WAIT {

				minReplicationReplies, _ := strconv.Atoi(cmd.Args[0])
				waitTime, _ := strconv.Atoi(cmd.Args[1])

				ackEventChan := ctx.Value(info.CTX_ACK_EVENT).(chan NotifyReplicationAck)
				log.Println("sending ack to replication", ackEventChan)
				ackEventChan <- NotifyReplicationAck{
					timeout:         waitTime,
					minimumNotifs:   minReplicationReplies,
					redisClientConn: conn,
				}
				shouldclose = false
				break OuterLoop
			} else {

				resp, shouldRegister := rs.getCmdResponse(&cmd, ctx)
				log.Printf("response to %+v:\n%s\n", cmd, resp)
				if resp != nil {
					conn.Write(resp)
				}
				if shouldRegister {
					registrationChan := ctx.Value(info.CTX_REPLACATION_REGISTRATION).(chan net.Conn)
					registrationChan <- conn
					shouldclose = false
					log.Println("register replica", conn.RemoteAddr())
					break OuterLoop
				}
			}
		case parser.SimpleString:
			log.Println("got simple string\n", incoming)
		case parser.UnknownData:
			log.Printf("got unknown data,\ndata type: '%c' \ndata: '%s'",
				incoming.(parser.UnknownData).Dt, string(incoming.(parser.UnknownData).Data))
		}
	}

	if shouldclose {
		log.Println("clossing ", conn.RemoteAddr())
		conn.Close()
	} else {
		log.Println("releaseing replication conn", conn.RemoteAddr())
	}

}

func (rs *RedisService) getCmdResponse(cmdInfo *parser.CmdInfo, ctx context.Context) ([]byte, bool) {

	serverInfo := ctx.Value(info.CTX_SERVER_INFO).(info.ServerInfo)
	switch cmdInfo.CmdName {
	case PING:
		return respencoding.EncodeSimpleString("PONG"), false
	case ECHO:
		return respencoding.EncodeSimpleString(cmdInfo.Args[0]), false
	case SET:
		if serverInfo[info.SERVER_ROLE] == info.ROLE_MASTER {
			cmdEvent := ctx.Value(info.CTX_REPLICATION_EVENTS).(chan parser.CmdInfo)
			cmdEvent <- *cmdInfo
		}

		if len(cmdInfo.Args) < 2 {
			return respencoding.EncodeSimpleError("Not enough args for SET: " + strings.Join(cmdInfo.Args, ",")), false
		}
		ok := false

		key := cmdInfo.Args[0]
		val := cmdInfo.Args[1]
		if len(cmdInfo.Args) > 2 {

			ops, err := buildKvsOptions(cmdInfo.Args[2:])
			if err != nil {
				return respencoding.EncodeSimpleError(err.Error()), false
			}
			ok = rs.kvs.SetWithOptions(key, []byte(val), ops)
		} else {
			ok = rs.kvs.Set(key, []byte(val))
		}
		if ok {

			return respencoding.EncodeSimpleString("OK"), false
		} else {
			return respencoding.EncodeSimpleError("ERR: could not store k/v"), false
		}
	case GET:
		value, ok := rs.kvs.Get(cmdInfo.Args[0])
		if ok {
			return respencoding.EncodeBulkStringArray([][]byte{value}), false
		} else {
			return []byte(NULL_BULK), false
		}
	case INFO:
		if len(cmdInfo.Args) < 1 {
			return respencoding.EncodeBulkString(info.BuildInfo("", ctx)), false
		} else {
			return respencoding.EncodeBulkString(info.BuildInfo(cmdInfo.Args[0], ctx)), false
		}
	case REPLCONF:
		if cmdInfo.Args[0] == "getack" {
			ackRply := [][]byte{
				[]byte(REPLCONF),
				[]byte("ack"),
				{'0'},
			}
			return respencoding.EncodeArray(ackRply), false
		}

		log.Println("Replication config received", cmdInfo)
		return respencoding.EncodeSimpleString("OK"), false
	case PSYNC:
		log.Println("Psync received", cmdInfo)
		resync := respencoding.EncodeSimpleString("FULLRESYNC " + serverInfo[info.SERVER_MASTER_REPLID] + " 0")
		rdbFile := rdb.BuildRDBFromMemory()
		rdbFileSize := strconv.Itoa(len(rdbFile))
		reply := make([]byte, 0, len(resync)+len(rdbFile)+10)
		reply = append(reply, resync...)
		reply = append(reply, '$')
		reply = append(reply, rdbFileSize...)
		reply = append(reply, []byte(parser.CRNL)...)
		reply = append(reply, rdbFile...)

		return reply, true
	case WAIT:
		return nil, false
	case CONFIG:

		if cmdInfo.Args[0] == "GET" {
			val := serverInfo[cmdInfo.Args[1]]
			if val != "" {
				resp := [][]byte{
					[]byte(cmdInfo.Args[1]),
					[]byte(val),
				}
				return respencoding.EncodeArray(resp), false
			}
		}
		return []byte(NULLS), false

	case KEYS:
		if cmdInfo.Args[0] == "*" {
			kvsKeys := rs.kvs.Keys()
			return respencoding.EncodeArray(kvsKeys), false
		}
	case TYPE:
		key := cmdInfo.Args[0]
		valueType := rs.kvs.GetType(key)
		return respencoding.EncodeSimpleString(valueType), false
	case XADD:
		if len(cmdInfo.Args) < 3 {
			return respencoding.EncodeSimpleError("Not enough data"), false
		}
		data := make(map[string]any, 1)
		for i := 2; i < len(cmdInfo.Args); i += 2 {
			data[cmdInfo.Args[i]] = cmdInfo.Args[i+1]
		}
		stored, error := rs.kvs.SetStream(cmdInfo.Args[0], cmdInfo.Args[1], data)

		if error != nil {
			return respencoding.EncodeSimpleError(error.Error()), false
		}
		if stored != "" {
			return respencoding.EncodeBulkString([]byte(stored)), false
		} else {
			return respencoding.EncodeSimpleError("ERR the stream as not saved"), false
		}
	case XRANGE:
		stream := rs.kvs.GetStream(cmdInfo.Args[0])
		var lowerRange, upperRange KvsStreamId
		if len(cmdInfo.Args) == 3 {

			lowerString := cmdInfo.Args[1]
			upperString := cmdInfo.Args[2]
			lowerMilli, err := strconv.ParseInt(lowerString, 10, 64)

			if err != nil {
				if cmdInfo.Args[1] == "-" {
					lowerRange = KvsStreamId{}
				} else {
					lowerRange, err = newStreamId(lowerString)
					if err != nil {
						return respencoding.EncodeSimpleError("ERR invalid lower range id " + err.Error()), false
					}
				}
			} else {
				lowerRange = KvsStreamId{milli: lowerMilli}
			}
			upperMilli, err := strconv.ParseInt(upperString, 10, 64)
			if err != nil {
				if cmdInfo.Args[2] == "+" {
					upperRange = KvsStreamId{milli: math.MaxInt64, sequence: math.MaxInt}
				} else {

					upperRange, err = newStreamId(upperString)
					if err != nil {
						return respencoding.EncodeSimpleError("ERR invalid upper range arg " + err.Error()), false
					}
				}
			} else {
				upperRange = KvsStreamId{milli: upperMilli}
			}
		}

		return stream.GetXRange(&lowerRange, &upperRange), false
	case XREAD:

		if cmdInfo.Args[0] == "streams" {

			key := cmdInfo.Args[1]

			from, err := newStreamId(cmdInfo.Args[2])

			if err != nil {
				return respencoding.EncodeSimpleError(err.Error()), false
			}

			stream := rs.kvs.GetStream(key)

			xreadResp := make([][]byte, 0, 2)

			xreadResp = append(xreadResp, respencoding.EncodeBulkString([]byte(key)))
			xreadResp = append(xreadResp, respencoding.BuildArray([][]byte{stream.GetXRead(&from)}))

			return respencoding.BuildArray([][]byte{respencoding.BuildArray(xreadResp)}), false
		}

		return respencoding.EncodeSimpleError(cmdInfo.Args[0] + " is not a valid read option"), false

	}

	return respencoding.EncodeSimpleString("UNKNOWN CMD"), false
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
