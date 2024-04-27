package parser

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	//parsing
	CRNL = "\r\n"
	LN   = '\n'

	//RESP
	RESP_ARRAY         DataType = '*'
	RESP_BULK_STRING   DataType = '$'
	RESP_SIMPLE_STRING DataType = '+'

	// CMD info
)

// RESP data type
type DataType byte

type Parser struct {
	input *bufio.Reader
}

// marker interface design pattern
// golimorphism I guess
// zoomers will not understand... design patterns KEKW
type RespResponse interface {
	respResponseType()
}

type CmdInfo struct {
	CmdName string
	Args    []string
}

func (cmdInfo CmdInfo) respResponseType() {}

type UnknownData struct {
	Dt   DataType
	Data []byte
}

func (uk UnknownData) respResponseType() {}

type SimpleString struct {
	Data string
}

func (ss SimpleString) respResponseType() {}

func NewParser(reader *bufio.Reader) *Parser {
	return &Parser{input: reader}
}

func (p *Parser) GetCmdInfo() (*CmdInfo, error) {

	res, err := p.ParseIncomingData()

	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("could not parse incomind data, err:%s", err)
	}

	if cmdInfo, ok := res.(CmdInfo); ok {
		return &cmdInfo, nil
	} else {
		return nil, fmt.Errorf("Incoming data is not a cmd, got:%T ", res)
	}
}

func (p *Parser) GetSimpleStringResponse() (*SimpleString, error) {

	res, err := p.ParseIncomingData()

	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("could not parse incomind data, err:%s", err)
	}

	if simpleString, ok := res.(SimpleString); ok {
		return &simpleString, nil
	} else {
		return nil, fmt.Errorf("Incoming data is not a cmd, got:%T ", res)
	}
}

func (p *Parser) ParseIncomingData() (RespResponse, error) {

	dataType, err := p.input.ReadByte()

	if err != nil {

		if err == io.EOF {
			return CmdInfo{}, err
		}
		return CmdInfo{}, fmt.Errorf("Error reading data type %s", err)
	}

	switch DataType(dataType) {

	case RESP_ARRAY:
		arraySizeByte, err := p.input.ReadString(LN)
		arraySizeByte = strings.TrimRight(arraySizeByte, CRNL)

		if err != nil {

			return CmdInfo{}, fmt.Errorf("Could not read array size: %s", err)
		}
		arraySize, err := strconv.Atoi(arraySizeByte)

		if err != nil {
			return CmdInfo{}, fmt.Errorf("Could not read array size: %s", err)

		}

		lines, err := p.bulkStringToStringSlice(arraySize)

		if err != nil {
			return CmdInfo{}, fmt.Errorf("Could not get cmd lines: %s", err)

		}

		cmdName, err := getCmdName(lines)

		if err != nil {
			return CmdInfo{}, fmt.Errorf("cmd name error: %s", err)
		}
		return CmdInfo{CmdName: cmdName, Args: lines[1:]}, nil
	case RESP_SIMPLE_STRING:
		data, err := p.input.ReadString(LN)

		if err != nil {
			return SimpleString{}, fmt.Errorf("error parsing simple string: %s", err)
		}

		return SimpleString{data}, nil
	default:
		unknowData := make([]byte, p.input.Buffered())
		p.input.Read(unknowData)

		return UnknownData{Dt: DataType(dataType), Data: unknowData}, nil
	}
}

func (p *Parser) bulkStringToStringSlice(size int) ([]string, error) {
	res := make([]string, 0, size)
	for range size {
		peekOne, err := p.input.Peek(1)

		if err != nil {
			return nil, fmt.Errorf("Could not peek: %s", err)
		}

		if isBulkStringDataType(peekOne[0]) {

			lineByte, err := p.input.ReadString(LN)

			if err != nil {
				return nil, fmt.Errorf("Could read bulk string first line: %s", err)
			}

			lineByte = strings.TrimRight(lineByte, CRNL)
			lineByte = strings.TrimLeft(lineByte, string(RESP_BULK_STRING))
			dataSize, err := strconv.Atoi(lineByte)

			if err != nil {
				return nil, fmt.Errorf("data size atoi err: %s", err)
			}

			dataString, err := p.input.ReadString(LN)
			dataString = strings.TrimRight(dataString, CRNL)

			if !(len(dataString) == dataSize) {
				return nil, fmt.Errorf("data size does not match data, size:%d data:%s", dataSize, dataString)
			}

			res = append(res, dataString)
		}
	}
	return res, nil
}

func asciiByteToInt(b byte) (int, error) {
	if b < '0' || b > '9' {
		return 0, fmt.Errorf("invalid ASCII digit: %c", b)
	}
	return int(b - '0'), nil
}

func isBulkStringDataType(b byte) bool {
	return DataType(b) == RESP_BULK_STRING
}

func getCmdName(cmdLines []string) (string, error) {

	if len(cmdLines) > 0 {
		return strings.ToLower(cmdLines[0]), nil
	}

	return "", fmt.Errorf("cmd lines might not be cmd data")
}
