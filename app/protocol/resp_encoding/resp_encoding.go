package respencoding

import (
	"fmt"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/protocol/parser"
)

func EncodeArray(s [][]byte) []byte {
	arrayContent := EncodeBulkStringArray(s)
	res := make([]byte, 0, len(arrayContent)+10)
	res = append(res, '*')
	res = append(res, []byte(strconv.Itoa(len(s)))...)
	res = append(res, []byte(parser.CRNL)...)
	res = append(res, arrayContent...)

	return res
}

func EncodeBulkString(s []byte) []byte {

	res := make([]byte, 0, len(s)+5)
	res = append(res, '$')
	res = append(res, []byte(strconv.Itoa(len(s)))...)
	res = append(res, '\r')
	res = append(res, '\n')
	res = append(res, s...)
	res = append(res, '\r')
	res = append(res, '\n')

	return res
}

func EncodeBulkStringArray(s [][]byte) []byte {

	buffer := make([]byte, 0, 1024)
	for _, i := range s {
		buffer = append(buffer, EncodeBulkString(i)...)
	}

	return buffer
}

func EncodeSimpleString(s string) []byte {
	byteResp := make([]byte, 0, len(s)+3)
	byteResp = append(byteResp, '+')
	byteResp = append(byteResp, addCRNL(s)...)
	return byteResp
}

func EncodeInteger(i int) []byte {
	intString := strconv.Itoa(i)
	byteResp := make([]byte, 0, len(intString)+3)
	byteResp = append(byteResp, ':')
	byteResp = append(byteResp, []byte(addCRNL(intString))...)
	return byteResp
}

func EncodeSimpleError(s string) []byte {

	byteResp := make([]byte, 0, len(s)+3)
	byteResp = append(byteResp, '-')
	byteResp = append(byteResp, addCRNL(s)...)
	return byteResp
}

func addCRNL(s string) []byte {
	return []byte(fmt.Sprintf("%s\r\n", s))
}
