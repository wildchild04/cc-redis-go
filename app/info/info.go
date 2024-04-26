package info

const (
	// Ctx value
	CTX_SERVER_INFO = "server-info"
	CTX_SESSION_ID  = "session-id"

	// Server inf
	SERVER_ROLE        = "role"
	SERVER_PORT        = "port"
	SERVER_MASTER_HOST = "master-host"
	SERVER_MASTER_PORT = "master-port"

	// role types
	ROLE_MASTER = "master"
	ROLE_SLAVE  = "slave"
)

type ServerInfo map[string]string
