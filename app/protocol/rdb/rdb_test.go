package rdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_buildRDB(t *testing.T) {

}

func Test_encodeString(t *testing.T) {
	tests := []struct {
		input         string
		expected      []byte
		errorExpected bool
	}{
		{
			input:    "test1234test",
			expected: []byte{0x0c, 116, 101, 115, 116, 49, 50, 51, 52, 116, 101, 115, 116},
		},
	}

	for _, tc := range tests {
		got, err := encodeString(tc.input)
		if tc.errorExpected {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
		}
		assert.Equal(t, tc.expected, got)
		fmt.Println(cap(got))
	}
}
