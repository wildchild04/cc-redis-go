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
	}

	for _, tc := range tests {
		got := getCmdResponse(tc.input)

		assert.Equal(t, tc.expected, got)
	}

}
