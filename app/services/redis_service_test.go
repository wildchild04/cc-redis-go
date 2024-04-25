package services

import (
	"testing"

	"github.com/codecrafters-io/redis-starter-go/app/protocol/parser"
	"github.com/stretchr/testify/assert"
)

func Test_getCmdResponse(t *testing.T) {

	tests := []struct {
		input    parser.CmdInfo
		expected []byte
		store    map[string][]byte
	}{
		{
			input: parser.CmdInfo{
				CmdName: PING,
			},
			expected: []byte("+PONG\r\n"),
		},
		{
			input: parser.CmdInfo{
				CmdName: ECHO,
				Args:    []string{"test"},
			},
			expected: []byte("+test\r\n"),
		},
		{
			input: parser.CmdInfo{
				CmdName: GET,
				Args:    []string{"test"},
			},
			store: map[string][]byte{
				"test": []byte("pog"),
			},
			expected: []byte("$3\r\npog\r\n"),
		},
		{
			input: parser.CmdInfo{
				CmdName: SET,
				Args:    []string{"test", "pog"},
			},
			store:    map[string][]byte{},
			expected: []byte("+OK\r\n"),
		},
	}

	for _, tc := range tests {
		rs := RedisService{&KvSMock{tc.store}}
		got := rs.getCmdResponse(tc.input)

		assert.Equal(t, tc.expected, got)
	}

}

type KvSMock struct {
	store map[string][]byte
}

func (kvs *KvSMock) Get(k string) ([]byte, bool) {
	v, ok := kvs.store[k]
	return v, ok
}

func (kvs *KvSMock) Set(k string, v []byte) bool {
	kvs.store[k] = v
	return true
}
