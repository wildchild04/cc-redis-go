package rdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_LoadRDBFile(t *testing.T) {

	tests := []struct {
		input  []byte
		expect RDBFile
	}{
		{
			input: []byte{
				82, 69, 68, 73, 83, 48, 48, 48, 51, 250, 9, 114, 101, 100,
				105, 115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, 250, 10,
				114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64, 254,
				0, 251, 1, 0, 0, 10, 115, 116, 114, 97, 119, 98, 101, 114, 114,
				121, 9, 114, 97, 115, 112, 98, 101, 114, 114, 121, 255, 125, 62,
				32, 84, 155, 95, 83, 163, 10,
			},
			expect: RDBFile{
				Kv: []RDBSimplePair{
					{
						Key:   "strawberry",
						Value: "raspberry",
					},
				},
			},
		}, {
			input: []byte{
				82, 69, 68, 73, 83, 48, 48, 48, 51, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114,
				5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64,
				254, 0, 251, 4, 0, 0, 9, 114, 97, 115, 112, 98, 101, 114, 114, 121, 5, 103, 114, 97, 112,
				101, 0, 5, 103, 114, 97, 112, 101, 6, 111, 114, 97, 110, 103, 101, 0, 6, 98, 97, 110, 97,
				110, 97, 6, 98, 97, 110, 97, 110, 97, 0, 5, 97, 112, 112, 108, 101, 10, 115, 116, 114, 97,
				119, 98, 101, 114, 114, 121, 255, 242, 181, 99, 94, 134, 64, 38, 118, 10,
			},
			expect: RDBFile{
				Kv: []RDBSimplePair{
					{Key: "raspberry", Value: "grape"},
					{Key: "grape", Value: "orange"},
					{Key: "banana", Value: "banana"},
					{Key: "apple", Value: "strawberry"},
				},
			},
		},
	}

	for _, tc := range tests {
		file, err := LoadRDBFile(tc.input)
		assert.Nil(t, err)
		assert.Equal(t, &tc.expect, file)
	}

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
	}
}
