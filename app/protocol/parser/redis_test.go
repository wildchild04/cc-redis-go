package parser

import (
	"bufio"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_bulkStringToStringSlice(t *testing.T) {

	tests := []struct {
		input         string
		size          int
		expected      []string
		expectedError bool
	}{
		{
			input:    "$4\r\ntest\r\n",
			size:     1,
			expected: []string{"test"},
		},
		{
			input:    "$4\r\necho\r\n$3\r\nhey\r\n",
			size:     2,
			expected: []string{"echo", "hey"},
		},
		{
			input:    "$10\r\nstrawberry\r\n$3\r\nhey\r\n",
			size:     2,
			expected: []string{"strawberry", "hey"},
		},
	}

	for _, tc := range tests {

		stringReader := strings.NewReader(tc.input)

		reader := bufio.NewReader(stringReader)
		parser := NewParser(reader)

		got, err, size := parser.bulkStringToStringSlice(tc.size)

		if tc.expectedError {
			assert.NotNil(t, err)
		}
		assert.Equal(t, len(tc.input), size)
		assert.Equal(t, tc.expected, got)
	}
}

func Test_getCmdInfo(t *testing.T) {

	tests := []struct {
		input       string
		expected    CmdInfo
		expectError bool
	}{
		{

			input: "*1\r\n$4\r\nping\r\n",
			expected: CmdInfo{
				CmdName: "ping",
				Args:    []string{},
				Size:    14,
			},
		},
		{
			input: "*2\r\n$4\r\necho\r\n$3\r\nhey\r\n",
			expected: CmdInfo{
				CmdName: "echo",
				Args:    []string{"hey"},
				Size:    23,
			},
		},
		{
			input: "*3\r\n$3\r\nset\r\n$4\r\ntest\r\n$3\r\npog\r\n",
			expected: CmdInfo{
				CmdName: "set",
				Args:    []string{"test", "pog"},
				Size:    32,
			},
		},
		{
			input: "*2\r\n$3\r\nget\r\n$4\r\ntest",
			expected: CmdInfo{
				CmdName: "get",
				Args:    []string{"test"},
				Size:    21,
			},
		},
	}

	for _, tc := range tests {

		stringReader := strings.NewReader(tc.input)

		reader := bufio.NewReader(stringReader)
		parser := NewParser(reader)

		got, err := parser.GetCmdInfo()

		if tc.expectError {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
		}

		assert.Equal(t, &tc.expected, got)
	}
}
