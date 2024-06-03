package services

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const NEVER_EXPIRE = -1

type Kvs interface {
	Set(k string, v []byte) bool
	SetWithOptions(k string, v []byte, ops KvsOptions) bool
	Get(k string) ([]byte, bool)
	GetType(k string) string
	Keys() [][]byte
	SetStream(k, id string, data map[string]any) (bool, error)
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
	fmt.Println(sid, id)
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

type KvsStreamObject struct {
	lastId KvsStreamId
	data   map[string]any
}

func (kvsStream KvsStreamObject) GetType() string {
	return "stream"
}

type kvSService struct {
	size  int64
	store *sync.Map
}

func NewKvSService() Kvs {
	return &kvSService{store: &sync.Map{}}
}

func (kvs *kvSService) SetStream(k, id string, data map[string]any) (bool, error) {
	streamObject, found := kvs.store.Load(k)
	currentStreamId, _ := newStreamId(id)
	if !found {
		kvs.store.Store(k, KvsStreamObject{lastId: currentStreamId, data: data})
	} else {
		stream, ok := streamObject.(KvsStreamObject)
		if !ok {
			return false, fmt.Errorf("Could not verify stream")
		}
		_, err := stream.lastId.isValidSequence(currentStreamId)
		if err != nil {
			return false, err
		}

		for k, v := range data {
			stream.data[k] = v
		}
		stream.lastId = currentStreamId
		kvs.store.Store(k, stream)
	}

	return true, nil
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
