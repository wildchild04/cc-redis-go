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
		store    map[string]KvsObject
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
			store: map[string]KvsObject{
				"test": {
					data: []byte("pog"),
				},
			},
			expected: []byte("$3\r\npog\r\n"),
		},
		{
			input: parser.CmdInfo{
				CmdName: SET,
				Args:    []string{"test", "pog"},
			},
			store:    map[string]KvsObject{},
			expected: []byte("+OK\r\n"),
		},
		{
			input: parser.CmdInfo{
				CmdName: SET,
				Args:    []string{"test", "pog", "px", "100"},
			},
			store:    map[string]KvsObject{},
			expected: []byte("+OK\r\n"),
		},
		{
			input: parser.CmdInfo{
				CmdName: SET,
				Args:    []string{"test", "pog", "px"},
			},
			store:    map[string]KvsObject{},
			expected: []byte("-Missing PX value\r\n"),
		},
	}

	for _, tc := range tests {
		rs := RedisService{&KvSMock{tc.store}}
		got := rs.getCmdResponse(tc.input)

		assert.Equal(t, tc.expected, got)
	}

}

type KvSMock struct {
	store map[string]KvsObject
}

func (kvs *KvSMock) Get(k string) ([]byte, bool) {

	v, ok := kvs.store[k]
	return v.data, ok
}

func (kvs *KvSMock) Set(k string, v []byte) bool {
	kvs.store[k] = KvsObject{data: v}
	return true
}

func (kvs *KvSMock) SetWithOptions(k string, v []byte, op KvsOptions) bool {
	return true
}
