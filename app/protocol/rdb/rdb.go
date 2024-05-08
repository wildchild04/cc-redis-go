package rdb

import (
	"encoding/base64"
	"fmt"
)

const (
	MAGIC_NUMBER          = "REDIS"
	VERSION               = "0003"
	EOF                   = 0xFF
	AUXILIAR_FILED_HEADER = 0xFA
	DB_SELECTOR           = 0xFE
)

func BuildRDB() []byte {

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
