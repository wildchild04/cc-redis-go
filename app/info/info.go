package info

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/protocol/parser"
)

const (
	// Ctx value
	CTX_SERVER_INFO              = "server-info"
	CTX_SESSION_ID               = "session-id"
	CTX_METRICS                  = "metrics"
	CTX_REPLICATION_EVENTS       = "replication-events"
	CTX_REPLACATION_REGISTRATION = "replication-registration"

	// Server inf
	SERVER_ROLE               = "role"
	SERVER_PORT               = "port"
	SERVER_MASTER_HOST        = "master_host"
	SERVER_MASTER_PORT        = "master_port"
	SERVER_MASTER_REPLID      = "master_replid"
	SERVER_MASTER_REPL_OFFSET = "master_repl_offset"

	// role types
	ROLE_MASTER = "master"
	ROLE_SLAVE  = "slave"

	INFO_REPL = "replication"
)

type ServerInfo map[string]string

type Metrics struct {
	mx               sync.Mutex
	replOffset       int64
	replicationCount int
}

func NewMetrics() *Metrics {
	return &Metrics{mx: sync.Mutex{}}
}

func (m *Metrics) AddToOffset(add int64) {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.replOffset += add
}

func (m *Metrics) GetReplOffset() int64 {
	m.mx.Lock()
	defer m.mx.Unlock()

	return m.replOffset
}

func (m *Metrics) PlusReplicationCount() {
	m.mx.Lock()
	defer m.mx.Unlock()
	m.replicationCount++
}

func (m *Metrics) GetReplicationCount() int {
	m.mx.Lock()
	defer m.mx.Unlock()
	return m.replicationCount
}

func BuildInfo(variant string, ctx context.Context) []byte {

	si := ctx.Value(CTX_SERVER_INFO).(ServerInfo)
	metrics := ctx.Value(CTX_METRICS).(*Metrics)

	var infoStrings []string
	switch variant {
	case INFO_REPL:
		infoStrings = []string{
			"#" + INFO_REPL,
			buildInfoPair(SERVER_ROLE, si[SERVER_ROLE]),
			buildInfoPair(SERVER_MASTER_REPLID, si[SERVER_MASTER_REPLID]),
			buildInfoPair(SERVER_MASTER_REPL_OFFSET, strconv.FormatInt(metrics.GetReplOffset(), 10)),
			parser.CRNL,
		}
	default:
		infoStrings = []string{
			buildInfoPair(SERVER_PORT, si[SERVER_PORT]),
			buildInfoPair(SERVER_ROLE, si[SERVER_ROLE]),
			parser.CRNL,
		}
	}
	return []byte(strings.Join(infoStrings, parser.CRNL))

}

func buildInfoPair(k, v string) string {
	return fmt.Sprintf("%s:%s", k, v)
}
