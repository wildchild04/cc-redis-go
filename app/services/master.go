package services

import (
	"bufio"
	"io"
	"log"
	"net"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/info"
	"github.com/codecrafters-io/redis-starter-go/app/protocol/parser"
	respencoding "github.com/codecrafters-io/redis-starter-go/app/protocol/resp_encoding"
)

type MasterService interface {
	HandleEvents()
	GetReplicationCmdChan() chan parser.CmdInfo
	GetReplicaRegistrationChan() chan net.Conn
	GetAckEventChan() chan NotifyReplicationAck
}

type NotifyReplicationAck struct {
	minimumNotifs   int
	timeout         int
	redisClientConn net.Conn
}

type masterServiceImpl struct {
	replicationEventChan    chan parser.CmdInfo
	replicaRegistrationChan chan net.Conn
	ackEventChan            chan NotifyReplicationAck
	ackRepliedEventChan     chan struct{}
	serverConnChan          chan net.Conn
	replicaConns            []net.Conn
	metrics                 *info.Metrics
	replicationHappened     bool
}

func NewMasterService(metrics *info.Metrics, scc chan net.Conn) MasterService {

	return &masterServiceImpl{
		replicationEventChan:    make(chan parser.CmdInfo),
		replicaRegistrationChan: make(chan net.Conn),
		ackEventChan:            make(chan NotifyReplicationAck),
		ackRepliedEventChan:     make(chan struct{}),
		serverConnChan:          scc,
		replicaConns:            make([]net.Conn, 0, 2),
		metrics:                 metrics,
	}
}

func (m *masterServiceImpl) GetAckEventChan() chan NotifyReplicationAck {
	return m.ackEventChan
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
			go m.handleReplicationConn(conn)
		case ackEvent := <-m.ackEventChan:
			m.sendReplicationAck()
			m.handleWaitCmd(ackEvent)
		}
	}
}

func (m *masterServiceImpl) handleReplicationConn(conn net.Conn) {

	log.Println("handling replication for", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	p := parser.NewParser(reader)

	for {
		incoming, err := p.ParseIncomingData()
		if err != nil {
			if err == io.EOF {
				log.Println("done with replica", conn.RemoteAddr())
				return
			}
			log.Println("replication parse error", err)
		}

		switch incoming.(type) {
		case parser.CmdInfo:
			log.Print(incoming)
			cmdInfo := incoming.(parser.CmdInfo)
			switch cmdInfo.CmdName {

			case REPLCONF:
				if cmdInfo.Args[0] == "getack" {
					ackRply := [][]byte{
						[]byte(REPLCONF),
						[]byte("ack"),
						{'0'},
					}
					conn.Write(respencoding.EncodeArray(ackRply))
				}

				if cmdInfo.Args[0] == "ACK" {
					log.Println("ACK revieced ", cmdInfo)
					log.Println("current offset", m.metrics.GetReplOffset())
					m.metrics.PlusReplicationCount()
					m.ackRepliedEventChan <- struct{}{}
					log.Println("FFs")

				}
			}
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

	size := 0
	for _, conn := range m.replicaConns {
		res := respencoding.EncodeArray(cmdStrings)
		conn.Write(res)
		size = len(res)
	}
	m.metrics.AddToOffset(int64(size))
	m.replicationHappened = true
}

func (m *masterServiceImpl) registerReplica(conn net.Conn) {

	m.replicaConns = append(m.replicaConns, conn)
	log.Printf("Connection from %s registered", conn.RemoteAddr())
	m.metrics.PlusReplicationCount()

}

func (m *masterServiceImpl) sendReplicationAck() {

	replAck := [][]byte{
		[]byte("REPLCONF"),
		[]byte("GETACK"),
		{'*'},
	}

	ack := respencoding.EncodeArray(replAck)
	m.metrics.ResetReplicationCount()
	for _, conn := range m.replicaConns {
		log.Println("sending repl ack to", conn.RemoteAddr())
		conn.Write(ack)
	}

	m.metrics.AddToOffset(int64(len(ack)))
}

func (m *masterServiceImpl) handleWaitCmd(n NotifyReplicationAck) {

	if !m.replicationHappened {

		n.redisClientConn.Write(respencoding.EncodeInteger(len(m.replicaConns)))
	} else {

		timeoutChan := make(chan struct{})
		totalEvents := 0
		log.Println("handling wait", n)
		go func() {
			log.Println("before sleep")
			time.Sleep(time.Duration(n.timeout) * time.Millisecond)
			timeoutChan <- struct{}{}
			log.Println("sleep done")
		}()
	WaitLoop:
		for {
			select {
			case <-m.ackRepliedEventChan:
				totalEvents++
				log.Println("total", totalEvents >= n.minimumNotifs)
				if totalEvents >= n.minimumNotifs {
					log.Println("minimum replication achieved")
					break WaitLoop
				}
			case <-timeoutChan:
				log.Println("timeout reached")
				break WaitLoop
			}
		}
		resp := respencoding.EncodeInteger(m.metrics.GetReplicationCount())
		n.redisClientConn.Write(resp)
	}
	log.Println("wait rep done to", n.redisClientConn.RemoteAddr())
	m.serverConnChan <- n.redisClientConn
}
