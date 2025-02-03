package redisdb

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strconv"
)

const (
	MAGIC_NUMBER         = "REDIS"
	VERSION              = "0003"
	EOF                  = 0xFF
	AUXILIAR_FILE_HEADER = 0xFA
	RESIZE_DB            = 0xFB
	DB_SELECTOR          = 0xFE
	EXPIRE_PAIR_4_BYTE   = 0xFD
	EXPIRE_PAIR_8_BYTE   = 0xFC
	RDB_END              = 0xFF
	REDIS_VERSION        = "redis-ver"
	REDIS_BITS           = "redis-bits"
	CREATE_TIME          = "ctime"
	USED_MEM             = "used-mem"
)

const (
	STRING_LENGTH_ENCODING_SINGLE_BYTE = 0
	STRING_LENGTH_ENCODING_DOUBLE_BYTE = 1
)

type RDBFile struct {
	Kv    []RDBSimplePair
	ExpKv []RDBExpirationPair
}

type RDBSimplePair struct {
	Key   string
	Value string
}

type RDBExpirationPair struct {
	Exp uint64
	RDBSimplePair
}

func LoadRDBFile(file []byte) (*RDBFile, error) {
	res := &RDBFile{Kv: make([]RDBSimplePair, 0, 1), ExpKv: make([]RDBExpirationPair, 0, 1)}

	currentMagicNumber := string(file[:len(MAGIC_NUMBER):len(MAGIC_NUMBER)])
	bytesRead := len(MAGIC_NUMBER)
	if !(currentMagicNumber == MAGIC_NUMBER) {
		return nil, fmt.Errorf("Error, magic number does not match, current magic number:%s, expected:%s", currentMagicNumber, MAGIC_NUMBER)
	}
	currentVersion := string(file[bytesRead : bytesRead+len(VERSION) : bytesRead+len(VERSION)])
	bytesRead += len(VERSION)

	if !(currentVersion == VERSION) {
		return nil, fmt.Errorf("Error, version does not match, current version:%s, expected:%s", currentVersion, VERSION)
	}

	nextSectionByte := file[bytesRead]

	if nextSectionByte == AUXILIAR_FILE_HEADER {
		_, auxBytesRead := readAuxiliarData(file[bytesRead:])
		bytesRead += auxBytesRead
	}

	dbSelector := file[bytesRead]

	if dbSelector == DB_SELECTOR {
		bytesRead++
		// dbNum := int(file[bytesRead])
		bytesRead++
	}

	for {
		if file[bytesRead] == RDB_END {
			break
		}
		resizeDb := file[bytesRead]

		if resizeDb == RESIZE_DB {
			bytesRead++
			numSize := getIntergerSize(file[bytesRead])
			// hashSize := decodeInteger(file, bytesRead, numSize)
			bytesRead += numSize
			numSize = getIntergerSize(file[bytesRead])
			// expHashSize := decodeInteger(file, bytesRead+1, getIntergerSize(file[bytesRead]))

			bytesRead += numSize
			if file[bytesRead] == 0 {
				bytesRead++
			}

		}

		switch file[bytesRead] {
		case EXPIRE_PAIR_4_BYTE:
		case EXPIRE_PAIR_8_BYTE:
			bytesRead++
			exp := decodeLong(file, bytesRead)
			bytesRead += 9
			key, val, read := readPair(file, bytesRead)
			bytesRead = read
			res.ExpKv = append(
				res.ExpKv,
				RDBExpirationPair{
					Exp:           exp,
					RDBSimplePair: RDBSimplePair{Key: key, Value: val},
				},
			)

		default:
			key := decodeString(file, bytesRead)
			bytesRead += len(key) + 1
			val := decodeString(file, bytesRead)
			bytesRead += len(val) + 1
			res.Kv = append(res.Kv, RDBSimplePair{Key: key, Value: val})
		}
		if file[bytesRead] == 0 {
			bytesRead++
		}
	}

	return res, nil
}

func readPair(file []byte, bytesRead int) (string, string, int) {

	key := decodeString(file, bytesRead)
	bytesRead += len(key) + 1
	val := decodeString(file, bytesRead)
	bytesRead += len(val) + 1
	return key, val, bytesRead
}

func readAuxiliarData(data []byte) (map[string]string, int) {
	bytesRead := 0

	result := make(map[string]string)
	for {
		if data[bytesRead] > 0xFB {
			break
		}
		if data[bytesRead] == AUXILIAR_FILE_HEADER {
			bytesRead++
		}
		dataKey := decodeString(data, bytesRead)
		bytesRead += len(dataKey) + 1
		switch dataKey {
		case REDIS_VERSION:
			version := decodeString(data, bytesRead)

			result[string(dataKey)] = version
			bytesRead += len(version) + 1
		case REDIS_BITS:
			redisBits := decodeInteger(data, bytesRead+1, getIntergerSize(data[bytesRead]))
			result[string(dataKey)] = redisBits
			bytesRead += 2
		default:
			bytesRead++
		}
	}

	return result, bytesRead
}

func BuildRDBFromFileSystem(reader io.Reader, size int64) []byte {

	buffer := make([]byte, size)

	bytesRead, err := reader.Read(buffer)

	if err != nil {
		if err == io.EOF {
			return buffer[:bytesRead]
		}
		log.Println("Error reading rbd file", err)
	}

	return buffer
}

func BuildRDBFromMemory() []byte {

	res := make([]byte, 0, 1024)
	//add MAGIC_NUMBER
	res = append(res, []byte(MAGIC_NUMBER)...)
	//version 0001
	res = append(res, []byte(VERSION)...)
	// add 0xFA	AUX

	//redis verison
	//aof-base 0\

	emptyFileBase64 := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

	data, err := base64.StdEncoding.DecodeString(emptyFileBase64)

	if err != nil {
		return []byte{}
	}

	return data
}

func getIntergerSize(size byte) int {
	msbs := size >> 6

	possibleSize := msbs & 0b11
	switch possibleSize {
	case 0b00:
		return 1
	case 0b01:
		return 2
	case 0b10:
		return 4
	}
	return 0
}

func decodeInteger(data []byte, index, size int) string {

	switch size {
	case 1:
		return strconv.Itoa(int(data[index]))
	default:
		return ""
	}
}

func decodeLong(data []byte, index int) uint64 {

	longbytes := data[index : index+8]
	long := binary.BigEndian.Uint64(longbytes)
	return long
}

func decodeString(data []byte, index int) string {
	stringLength := int(data[index])
	res := make([]byte, 0, stringLength)

	for i := 0; i <= stringLength; i++ {

		someByte := data[index+i]
		if someByte > 32 {
			res = append(res, someByte)
		}
	}
	return string(res)
}

func encodeString(s string) ([]byte, error) {

	res := make([]byte, 0, len(s)+1)

	size := byte(len(s))
	if size > 0xFF {
		return nil, fmt.Errorf("String size must fit one byte, exceded size:%d", size)
	}
	res = append(res, size)
	res = append(res, s...)

	return res, nil
}
