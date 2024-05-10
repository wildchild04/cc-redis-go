package services

import (
	"log"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/info"
	"github.com/codecrafters-io/redis-starter-go/app/protocol/parser"
	respencoding "github.com/codecrafters-io/redis-starter-go/app/protocol/resp_encoding"
)

type MasterService interface {
	HandleEvents()
	GetReplicationCmdChan() chan parser.CmdInfo
	GetReplicaRegistrationChan() chan net.Conn
}

type masterServiceImpl struct {
	replicationEventChan    chan parser.CmdInfo
	replicaRegistrationChan chan net.Conn
	replicaConns            []net.Conn
	metrics                 *info.Metrics
}

func NewMasterService(metrics *info.Metrics) MasterService {

	return &masterServiceImpl{
		replicationEventChan:    make(chan parser.CmdInfo),
		replicaRegistrationChan: make(chan net.Conn),
		replicaConns:            make([]net.Conn, 0, 2),
		metrics:                 metrics,
	}
}

func (m *masterServiceImpl) GetReplicationCmdChan() chan parser.CmdInfo {
	return m.replicationEventChan
}

func (m *masterServiceImpl) GetReplicaRegistrationChan() chan net.Conn {
	return m.replicaRegistrationChan
}

func (m *masterServiceImpl) HandleEvents() {

	for {
		select {
		case cmd := <-m.replicationEventChan:
			m.handleReplicationEvents(cmd)
		case conn := <-m.replicaRegistrationChan:
			m.registerReplica(conn)
		}
	}

}

func (m *masterServiceImpl) handleReplicationEvents(cmd parser.CmdInfo) {

	cmdStrings := make([][]byte, 0, 3)

	switch cmd.CmdName {

	case SET:
		cmdStrings = append(cmdStrings, []byte(cmd.CmdName))
		cmdStrings = append(cmdStrings, []byte(cmd.Args[0]))
		cmdStrings = append(cmdStrings, []byte(cmd.Args[1]))
	}

	for _, conn := range m.replicaConns {
		conn.Write(respencoding.EncodeArray(cmdStrings))
	}
}

func (m *masterServiceImpl) registerReplica(conn net.Conn) {

	m.replicaConns = append(m.replicaConns, conn)
	log.Printf("Connection from %s registered", conn.RemoteAddr())
	m.metrics.PlusReplicationCount()

}

func SendReplicationAck(conn net.Conn) {

	log.Println("sending repl ack to", conn.RemoteAddr())
	replAck := [][]byte{
		[]byte("REPLCONF"),
		[]byte("GETACK"),
		{'*'},
	}

	ack := respencoding.EncodeArray(replAck)
	log.Println("lel " + string(ack))
	conn.Write(ack)

}
