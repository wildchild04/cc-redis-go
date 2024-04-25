package cmd

type Cmd interface {
	reply() []byte
}

type PingCmd struct {
}

func (ping *PingCmd) reply() []byte {
	return []byte("PONG")
}
