package services

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
)

type ReplicationService interface {
	HandleMasterConn(con net.Conn, ctx context.Context)
}

type replicationServiceImp struct {
	kvnService Kvs
}

func NewReplicationService(kvs Kvs) ReplicationService {
	return &replicationServiceImp{kvnService: kvs}
}

func (r *replicationServiceImp) HandleMasterConn(conn net.Conn, ctx context.Context) {

	reader := bufio.NewReader(conn)
	parser := parser.NewParser(reader)
	ok, err := r.performMasterHandshake(conn, parser, ctx)
	if err != nil {
		log.Println("handshake error" + err.Error())
		return
	}

	if ok {
		log.Println("Handshake sucessful")

		go r.listenToMaster(conn, parser)
	}

}

func (r *replicationServiceImp) listenToMaster(conn net.Conn, p *parser.Parser) {

	log.Println("listen to master coms")
	for {
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
			r.handleMasterCmd(cmd, conn)
		case parser.RDBFile:
			log.Println("received rbd file")
		case parser.UnknownData:
			ud := incoming.(parser.UnknownData)
			log.Println("some incoming data\n", ud.Dt, string(ud.Data))
		}

	}
}

func (r *replicationServiceImp) handleMasterCmd(cmd parser.CmdInfo, conn net.Conn) {

	log.Println("handling replication from master", cmd)
	switch cmd.CmdName {
	case SET:
		r.kvnService.Set(cmd.Args[0], []byte(cmd.Args[1]))
	case REPLCONF:
		if cmd.Args[0] == "GETACK" {

			ack := [][]byte{
				[]byte("REPLCONF"),
				[]byte("ACK"),
				{'0'},
			}

			conn.Write(respencoding.EncodeArray(ack))
		}

	default:
		log.Println("could not handle unkwon cmd", cmd)
	}

}

func (r *replicationServiceImp) performMasterHandshake(conn net.Conn, p *parser.Parser, ctx context.Context) (bool, error) {

	serverInfo := ctx.Value(info.CTX_SERVER_INFO).(info.ServerInfo)
	conn.Write(respencoding.EncodeArray([][]byte{[]byte("ping")}))
	log.Println("Handshake started from", conn.RemoteAddr())
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
