package services

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	respencoding "github.com/codecrafters-io/redis-starter-go/app/protocol/resp_encoding"
)

const NEVER_EXPIRE = -1

type Kvs interface {
	Set(k string, v []byte) bool
	SetWithOptions(k string, v []byte, ops KvsOptions) bool
	Get(k string) ([]byte, bool)
	GetType(k string) string
	Keys() [][]byte
	SetStream(k, id string, data map[string]any) (string, error)
	GetStream(k string) *KvsStream
	SubscriveStreamEventListener(k string, listener chan string)
	UnsubscriveStreamEventListener(k string)
}

type KvsObject interface {
	GetType() string
}

type KvsOptions struct {
	expires   time.Duration
	timestamp uint64
}

type KvsStringObject struct {
	data    []byte
	created *time.Time
	expires *time.Time
}

func (kvsString KvsStringObject) GetType() string {
	return "string"
}

type KvsStreamId struct {
	milli    int64
	sequence int
}

func newStreamId(id string) (KvsStreamId, error) {
	idElements := strings.Split(id, "-")
	millis, err := strconv.ParseInt(idElements[0], 10, 64)
	sequence, err := strconv.Atoi(idElements[1])
	if err != nil {
		return KvsStreamId{}, fmt.Errorf("could not parse stream id %s, %w", id, err)
	}
	return KvsStreamId{milli: millis, sequence: sequence}, nil
}

func (sid KvsStreamId) String() string {
	return fmt.Sprintf("%d-%d", sid.milli, sid.sequence)
}

func (sid KvsStreamId) isValidSequence(id KvsStreamId) (bool, error) {
	if sid.milli > id.milli {
		if sid.sequence > id.sequence {
			return false, fmt.Errorf("ERR The ID specified in XADD must be greater than %s", id)
		}
		return false, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	if sid.milli == id.milli {
		if sid.sequence == id.sequence {
			return false, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}
	return true, nil
}

func (sid KvsStreamId) generateNextSequence(millis int64) KvsStreamId {
	if sid.milli == millis {
		return KvsStreamId{milli: millis, sequence: sid.sequence + 1}
	}
	return KvsStreamId{milli: millis, sequence: 0}
}

type KvsStream struct {
	lastId  KvsStreamId
	objects []KvsStreamObject
}

func (kvsStream KvsStream) GetType() string {
	return "stream"
}

func (kvss KvsStream) GetXRead(from *KvsStreamId) []byte {
	streamBytes := make([][]byte, 0, len(kvss.objects))
	for _, streamObject := range kvss.objects {
		if streamObject.id.milli > from.milli ||
			(streamObject.id.milli == from.milli &&
				streamObject.id.sequence > from.sequence) {
			if from.milli == -1 && kvss.lastId.milli >= streamObject.id.milli && kvss.lastId.sequence >= streamObject.id.sequence {
				continue
			}
			dataBytes := make([][]byte, 0, len(streamObject.data))
			for k, v := range streamObject.data {
				dataBytes = append(dataBytes, respencoding.EncodeBulkString([]byte(k)))
				dataBytes = append(dataBytes, respencoding.EncodeBulkString([]byte(v.(string))))
			}
			streamBytes = append(streamBytes, respencoding.EncodeBulkString([]byte(streamObject.id.String())))
			streamBytes = append(streamBytes, respencoding.BuildArray(dataBytes))
		}
	}
	return respencoding.BuildArray(streamBytes)
}

func (kvss KvsStream) GetXRange(lowerRange, upperRange *KvsStreamId) []byte {

	res := make([][]byte, 0, len(kvss.objects))
	for _, stream := range kvss.objects {
		streamBytes := make([][]byte, 0, 2)
		if stream.id.milli >= lowerRange.milli && stream.id.sequence >= lowerRange.sequence &&
			stream.id.milli <= upperRange.milli && stream.id.sequence <= upperRange.sequence {
			encodeId := respencoding.EncodeBulkString([]byte(stream.id.String()))
			dataBytes := make([][]byte, 0, len(stream.data))
			for k, v := range stream.data {
				dataBytes = append(dataBytes, []byte(k))
				dataBytes = append(dataBytes, []byte(v.(string)))
			}
			streamBytes = append(streamBytes, encodeId)
			streamBytes = append(streamBytes, respencoding.EncodeArray(dataBytes))
			res = append(res, respencoding.BuildArray(streamBytes))
		}
	}
	return respencoding.BuildArray(res)
}

type KvsStreamObject struct {
	id   KvsStreamId
	data map[string]any
}

func (kvsStream KvsStreamObject) GetRespEncodign() [][]byte {
	res := make([][]byte, 0, len(kvsStream.data)+1)
	res = append(res, []byte(kvsStream.id.String()))
	for k, v := range kvsStream.data {
		res = append(res, []byte(k))
		switch v.(type) {
		case string:
			res = append(res, []byte(v.(string)))
		}
	}
	return res
}

type kvSService struct {
	size           int64
	store          *sync.Map
	setStreamEvent map[string]chan string
}

func NewKvSService() Kvs {
	return &kvSService{store: &sync.Map{}, setStreamEvent: make(map[string]chan string)}
}

func (kvs *kvSService) UnsubscriveStreamEventListener(k string) {
	delete(kvs.setStreamEvent, k)
}

func (kvs *kvSService) SubscriveStreamEventListener(k string, listener chan string) {
	kvs.setStreamEvent[k] = listener
}

func (kvs *kvSService) GetStream(k string) *KvsStream {
	streamObject, found := kvs.store.Load(k)
	if found {
		stream, ok := streamObject.(KvsStream)
		if ok {
			return &stream
		}
	}
	return nil
}

func (kvs *kvSService) SetStream(k, id string, data map[string]any) (string, error) {
	streamObject, found := kvs.store.Load(k)

	prevId := ""
	var currentStreamId KvsStreamId
	var err error

	if id == "*" {
		currentStreamId = KvsStreamId{milli: time.Now().UnixMilli(), sequence: 0}
	} else if strings.Contains(id, "*") {
		currentStreamId, err = newStreamId(strings.Replace(id, "*", "1", -1))
	} else {
		currentStreamId, err = newStreamId(id)
	}

	if !found {

		if err != nil {
			currentStreamId = KvsStreamId{}
		}

		kvs.store.Store(k, KvsStream{lastId: currentStreamId, objects: []KvsStreamObject{
			{id: currentStreamId, data: data},
		}})
	} else {

		stream, ok := streamObject.(KvsStream)
		prevId = stream.lastId.String()
		if !ok {
			return "", fmt.Errorf("Could not verify stream")
		}

		if id == "*" {
			currentStreamId = stream.lastId.generateNextSequence(stream.lastId.milli)
		} else if strings.Contains(id, "*") {
			currentStreamId = stream.lastId.generateNextSequence(currentStreamId.milli)
		}
		_, err := stream.lastId.isValidSequence(currentStreamId)
		if err != nil {
			return "", err
		}

		stream.objects = append(stream.objects, KvsStreamObject{currentStreamId, data})
		stream.lastId = currentStreamId
		kvs.store.Store(k, stream)
	}

	go func() {
		kvs.setStreamEvent[k] <- fmt.Sprintf("%s,%s", k, prevId)
	}()

	return currentStreamId.String(), nil
}

func (kvs *kvSService) GetType(k string) string {
	res, ok := kvs.store.Load(k)
	if !ok {
		return "none"
	}
	obj, ok := res.(KvsObject)
	if !ok {
		return "none"
	}
	return obj.GetType()
}

func (kvs *kvSService) Set(key string, value []byte) bool {

	now := time.Now()
	object := KvsStringObject{
		data:    value,
		created: &now,
	}
	kvs.size++
	kvs.store.Store(key, object)

	return true
}

func (kvs *kvSService) SetWithOptions(key string, value []byte, options KvsOptions) bool {

	obj := KvsStringObject{
		data: value,
	}
	if options.expires != 0 {
		kvs.stampObject(&obj, options.expires)
	}

	if options.timestamp != 0 {

		t := convertTimestampToTime(int64(options.timestamp))
		if t.Before(time.Now()) {
			return false
		}
		created := time.Now()
		obj.created = &created
		obj.expires = &t

	}
	kvs.size++
	kvs.store.Store(key, obj)
	return true
}

func (kvs *kvSService) Get(k string) ([]byte, bool) {

	anyV, ok := kvs.store.Load(k)
	obj, ok := anyV.(KvsStringObject)

	if obj.expires != nil && obj.expires.Before(time.Now()) {
		return nil, false
	}

	return obj.data, ok
}

func (kvs *kvSService) Keys() [][]byte {
	res := make([][]byte, 0, kvs.size)

	kvs.store.Range(func(k, v any) bool {
		key := k.(string)
		res = append(res, []byte(key))
		return true
	})

	return res
}

func NewKvsOptionsWithTimestamp(timestamp uint64) KvsOptions {
	return KvsOptions{timestamp: timestamp}
}

func (kvs *kvSService) stampObject(ko *KvsStringObject, ex time.Duration) {
	created := time.Now()
	expires := created.Add(ex * time.Millisecond)

	ko.created = &created
	ko.expires = &expires

}

func convertTimestampToTime(timestamp int64) time.Time {
	var t time.Time
	if timestamp > 1e16 {
		seconds := timestamp / 1e9
		nanoseconds := timestamp % 1e9
		t = time.Unix(seconds, nanoseconds)
	} else {
		// Assuming timestamp is in microseconds
		seconds := timestamp / 1e6
		nanoseconds := (timestamp % 1e6) * 1e3
		t = time.Unix(seconds, nanoseconds)
	}
	return t
}
