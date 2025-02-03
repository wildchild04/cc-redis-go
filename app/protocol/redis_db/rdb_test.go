package redisdb

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
				ExpKv: []RDBExpirationPair{},
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
				ExpKv: []RDBExpirationPair{},
			},
		}, {
			input: []byte{
				82, 69, 68, 73, 83, 48, 48, 48, 51, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114,
				5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64,
				254, 0, 251, 3, 3, 252, 0, 156, 239, 18, 126, 1, 0, 0, 0, 4, 112, 101, 97, 114, 5, 103,
				114, 97, 112, 101, 252, 0, 12, 40, 138, 199, 1, 0, 0, 0, 9, 112, 105, 110, 101, 97, 112,
				112, 108, 101, 5, 109, 97, 110, 103, 111, 252, 0, 12, 40, 138, 199, 1, 0, 0, 0, 5, 109,
				97, 110, 103, 111, 4, 112, 101, 97, 114, 255, 31, 235, 107, 18, 213, 4, 42, 184, 10,
			},
			expect: RDBFile{
				Kv: []RDBSimplePair{},
				ExpKv: []RDBExpirationPair{
					{Exp: 0x9cef127e010000, RDBSimplePair: RDBSimplePair{Key: "pear", Value: "grape"}},
					{Exp: 0xc288ac7010000, RDBSimplePair: RDBSimplePair{Key: "pineapple", Value: "mango"}},
					{Exp: 0xc288ac7010000, RDBSimplePair: RDBSimplePair{Key: "mango", Value: "pear"}}},
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
