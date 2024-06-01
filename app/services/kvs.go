package services

import (
	"sync"
	"time"
)

const NEVER_EXPIRE = -1

type Kvs interface {
	Set(k string, v []byte) bool
	SetWithOptions(k string, v []byte, ops KvsOptions) bool
	Get(k string) ([]byte, bool)
	Keys() [][]byte
}

type KvsOptions struct {
	expires   time.Duration
	timestamp uint64
}

type KvsObject struct {
	data    []byte
	created *time.Time
	expires *time.Time
}
type kvSService struct {
	size  int64
	store *sync.Map
}

func NewKvSService() Kvs {
	return &kvSService{store: &sync.Map{}}
}

func (kvs *kvSService) Set(key string, value []byte) bool {

	now := time.Now()
	object := KvsObject{
		data:    value,
		created: &now,
	}
	kvs.size++
	kvs.store.Store(key, object)

	return true
}

func (kvs *kvSService) SetWithOptions(key string, value []byte, options KvsOptions) bool {

	obj := KvsObject{
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
	obj, ok := anyV.(KvsObject)

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

func (kvs *kvSService) stampObject(ko *KvsObject, ex time.Duration) {
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
