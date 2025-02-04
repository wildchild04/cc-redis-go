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
	redisdb "github.com/codecrafters-io/redis-starter-go/app/protocol/redis_db"
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
	INCR     = "incr"
	MULTI    = "multi"
	EXEC     = "exec"
	DISCARD  = "discard"

	//RESP3 reply
	NULLS     = "_\r\n"
	NULL_BULK = "$-1\r\n"

	//SET OPTIONS
	PX = "px"

	//info cmd
	REPLICATION = "#Replication"
)

type RedisService struct {
	multiQueue map[string][]*parser.CmdInfo
	kvs        Kvs
}

func NewRedisService(kvs Kvs, streamSetEven chan string) *RedisService {
	return &RedisService{kvs: kvs, multiQueue: make(map[string][]*parser.CmdInfo, 10)}
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
			} else if rs.multiQueue[conn.RemoteAddr().String()] != nil && cmd.CmdName != EXEC && cmd.CmdName != DISCARD {
				log.Printf("Adding cmd %s to queue %s\n", cmd.CmdName, rs.multiQueue[conn.RemoteAddr().String()])
				rs.multiQueue[conn.RemoteAddr().String()] = append(rs.multiQueue[conn.RemoteAddr().String()], &cmd)
				conn.Write(respencoding.EncodeSimpleString("QUEUED"))
			} else {

				if cmd.CmdName == EXEC || cmd.CmdName == MULTI || cmd.CmdName == DISCARD {
					cmd.Args = append(cmd.Args, conn.RemoteAddr().String())
				}

				shouldRegister := rs.writeResponse(conn, &cmd, ctx)
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

func (rs *RedisService) writeResponse(conn net.Conn, cmd *parser.CmdInfo, ctx context.Context) bool {
	resp, shouldRegister := rs.getCmdResponse(cmd, ctx)
	if resp != nil {
		log.Printf("response to %+v:\n%s\n", cmd, resp)
		conn.Write(resp)
	}
	return shouldRegister
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
		rdbFile := redisdb.BuildRDBFromMemory()
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
		streamCmd := false
		streamsIndex := 0
		var blockMilis int64
		blockMilis = -1
		for i, cmdOp := range cmdInfo.Args {
			switch cmdOp {
			case "streams":
				streamCmd = true
				streamsIndex = i + 1
			case "block":
				blockMilis, _ = strconv.ParseInt(cmdInfo.Args[i+1], 10, 64)
			}
		}

		if streamCmd {
			if blockMilis >= 0 {
				listener := make(chan string)
				k := cmdInfo.Args[streamsIndex]
				rs.kvs.SubscriveStreamEventListener(k, listener)

				if blockMilis > 0 {

					go func(millis int64) {
						time.Sleep(time.Duration(millis) * time.Millisecond)
						listener <- "none"
					}(blockMilis)
				}
				event := <-listener
				if event == "none" {
					rs.kvs.UnsubscriveStreamEventListener(k)
					return []byte(NULL_BULK), false
				} else {
					eventData := strings.Split(event, ",")
					return rs.xRead(cmdInfo.Args[streamsIndex:], eventData[1]), false
				}
			} else {
				return rs.xRead(cmdInfo.Args[streamsIndex:], ""), false
			}
		}
		return respencoding.EncodeSimpleError("invalid read cmd"), false

	case INCR:
		key := cmdInfo.Args[0]
		val, exist := rs.kvs.Get(key)
		if !exist {
			Kvs.Set(rs.kvs, key, []byte("1"))
			val = []byte("0")
		}
		num, err := strconv.Atoi(string(val))

		if err != nil {
			return respencoding.EncodeSimpleError("ERR value is not an integer or out of range"), false
		}

		num++
		numBytes := []byte(strconv.Itoa(num))
		rs.kvs.Set(key, numBytes)

		return respencoding.EncodeInteger(num), false

	case MULTI:
		remote := cmdInfo.Args[0]
		rs.multiQueue[remote] = make([]*parser.CmdInfo, 0, 10)
		return respencoding.EncodeSimpleString("OK"), false

	case EXEC:
		remote := cmdInfo.Args[0]
		if rs.multiQueue[remote] == nil {
			return respencoding.EncodeSimpleError("ERR EXEC without MULTI"), false
		}
		responses := make([][]byte, 0, len(rs.multiQueue[remote]))
		for _, cmd := range rs.multiQueue[remote] {
			resp, _ := rs.getCmdResponse(cmd, ctx)
			responses = append(responses, resp)

		}

		rs.multiQueue[remote] = nil
		return respencoding.BuildArray(responses), false

	case DISCARD:
		remote := cmdInfo.Args[0]
		_, exits := rs.multiQueue[remote]
		if exits {
			delete(rs.multiQueue, remote)
			return respencoding.EncodeSimpleString("OK"), false
		}

		return respencoding.EncodeSimpleError("ERR DISCARD without MULTI"), false

	}

	return respencoding.EncodeSimpleString("UNKNOWN CMD"), false
}

func (rs *RedisService) xRead(streams []string, prevId string) []byte {
	totalStreams := len(streams) / 2
	xreadResp := make([][]byte, 0, totalStreams)
	res := make([][]byte, 0, totalStreams)
	for i := 0; i < totalStreams; i++ {
		streamData := make([][]byte, 0, totalStreams)
		key := streams[i]
		var from KvsStreamId
		var err error
		if streams[totalStreams+i] == "$" {
			from, err = newStreamId(prevId)
		} else {
			from, err = newStreamId(streams[totalStreams+i])
		}
		if err != nil {
			return respencoding.EncodeSimpleError(err.Error())
		}
		stream := rs.kvs.GetStream(key)
		streamData = append(streamData, respencoding.EncodeBulkString([]byte(key)))
		streamData = append(streamData, respencoding.BuildArray([][]byte{stream.GetXRead(&from)}))
		res = append(res, respencoding.BuildArray(streamData))
	}

	for _, s := range res {
		xreadResp = append(xreadResp, s)
	}
	return respencoding.BuildArray(xreadResp)
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
