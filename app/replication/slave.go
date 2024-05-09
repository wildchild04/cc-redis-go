package replication

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/info"
	"github.com/codecrafters-io/redis-starter-go/app/protocol/parser"
	respencoding "github.com/codecrafters-io/redis-starter-go/app/protocol/resp_encoding"
	"github.com/codecrafters-io/redis-starter-go/app/services"
)

type SlaveService interface {
	RegisterReplica(conn net.Conn)
	HandleWriteCmd()
	GetEventChan() chan parser.CmdInfo
	GetReplicaRegistrationChan() chan net.Conn
	HandleSlaveConn(conn net.Conn, ctx context.Context)
}

type slaveServiceImp struct {
	slavesConns          []net.Conn
	replicationEventChan chan parser.CmdInfo
	registerReplica      chan net.Conn
	kvsService           services.Kvs
}

func (ss *slaveServiceImp) RegisterReplica(conn net.Conn) {
	ss.slavesConns = append(ss.slavesConns, conn)
	log.Printf("Connection from %s registered", conn.RemoteAddr())
}

func (ss *slaveServiceImp) GetEventChan() chan parser.CmdInfo {
	return ss.replicationEventChan
}

func (ss *slaveServiceImp) GetReplicaRegistrationChan() chan net.Conn {
	return ss.registerReplica
}

func NewSlaveService(repEventChan chan parser.CmdInfo, kvs services.Kvs) SlaveService {
	return &slaveServiceImp{
		slavesConns:          make([]net.Conn, 0, 5),
		replicationEventChan: repEventChan,
		registerReplica:      make(chan net.Conn),
		kvsService:           kvs,
	}
}

func (ss *slaveServiceImp) HandleWriteCmd() {
	for {
		select {
		case cmd := <-ss.replicationEventChan:

			forwardCmd := respencoding.EncodeArray([][]byte{[]byte(cmd.CmdName), []byte(cmd.Args[0]), []byte(cmd.Args[1])})

			for _, conn := range ss.slavesConns {
				conn.Write(forwardCmd)
			}
		case conn := <-ss.registerReplica:
			ss.RegisterReplica(conn)

		}
	}
}

func (ss *slaveServiceImp) handleCmdFromMaster(cmdInfo parser.CmdInfo) {

	switch cmdInfo.CmdName {
	case services.SET:
		ss.kvsService.Set(cmdInfo.Args[0], []byte(cmdInfo.Args[1]))

	}
}

func (ss *slaveServiceImp) HandleSlaveConn(conn net.Conn, ctx context.Context) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	parser := parser.NewParser(reader)
	ok, err := performMasterHandshake(conn, parser, ctx)
	if err != nil {
		log.Println("handshake error" + err.Error())
		return
	}

	if ok {
		log.Println("Handshake sucessful")
		ss.RegisterReplica(conn)
	}

	for {

		resp, err := parser.ParseIncomingData()

		if err != nil {
			if err == io.EOF {
				return
			}

			log.Println("Error getting cmd from master", err)
		} else {
			ss.handleResponse(resp)
		}
	}

}

func (ss *slaveServiceImp) handleResponse(response parser.RespResponse) {

	switch response.(type) {

	case parser.CmdInfo:
		ss.handleCmdFromMaster(response.(parser.CmdInfo))
		log.Println("got cmd info:", response)
	case parser.SimpleString:
		log.Println("got simple string\n", response)
	case parser.UnknownData:
		log.Printf("got unknown data,\ndata type: '%c' \ndata: '%s'",
			response.(parser.UnknownData).Dt, string(response.(parser.UnknownData).Data))
	}

}

func performMasterHandshake(conn net.Conn, p *parser.Parser, ctx context.Context) (bool, error) {

	serverInfo := ctx.Value(info.CTX_SERVER_INFO).(info.ServerInfo)
	conn.Write(respencoding.EncodeArray([][]byte{[]byte("ping")}))
	log.Println("Handshake started")
	pingResp, err := p.GetSimpleStringResponse()

	if err != nil {
		return false, fmt.Errorf("Handshake 1/3 failed: %s", err)
	}

	if pingResp.Data != "PONG\r\n" {
		return false, fmt.Errorf("Handshake 1/3 failed, invalid ping reply: '%s'", pingResp.Data)
	}

	replConfData := [][]byte{[]byte("REPLCONF"), []byte("listening-port"), []byte(serverInfo[info.SERVER_PORT])}
	replConf := respencoding.EncodeArray(replConfData)
	conn.Write(replConf)

	okRep, err := p.GetSimpleStringResponse()

	if err != nil {
		return false, fmt.Errorf("Handshake 2/3 failed: %s", err)
	}

	if okRep.Data != "OK\r\n" {
		return false, fmt.Errorf("Handshake 2/3 failed, invalid REPLCONF reply: '%s'", pingResp.Data)
	}

	replConfData = [][]byte{[]byte("REPLCONF"), []byte("capa"), []byte("psync2")}
	replConf = respencoding.EncodeArray(replConfData)
	conn.Write(replConf)

	okRep, err = p.GetSimpleStringResponse()

	if err != nil {
		return false, fmt.Errorf("Handshake 2/3 failed: %s", err)
	}

	if okRep.Data != "OK\r\n" {
		return false, fmt.Errorf("Handshake 2/3 failed, invalid REPLCONF reply: '%s'", pingResp.Data)
	}

	replConfData = [][]byte{[]byte("PSYNC"), []byte("?"), []byte("-1")}
	replConf = respencoding.EncodeArray(replConfData)
	conn.Write(replConf)

	fullSyncRep, err := p.GetSimpleStringResponse()

	if err != nil {
		return false, fmt.Errorf("Handshake 3/3 failed: %s", err)
	}

	fullSyncData := strings.TrimRight(fullSyncRep.Data, parser.CRNL)
	dataSlice := strings.Split(fullSyncData, " ")

	if len(dataSlice) != 3 && dataSlice[0] != "FULLSYNC" {
		return false, fmt.Errorf("Handshake 3/3 failed: invalid full sync reply %s", fullSyncData)
	}

	return true, nil
}
