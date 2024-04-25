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
}

type KvsOptions struct {
	expires time.Duration
}

type KvsObject struct {
	data    []byte
	created *time.Time
	expires *time.Time
}
type kvSService struct {
	store *sync.Map
}

func NewKvSService() Kvs {
	return &kvSService{&sync.Map{}}
}

func (kvs *kvSService) Set(key string, value []byte) bool {

	now := time.Now()
	object := KvsObject{
		data:    value,
		created: &now,
	}

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

func (kvs *kvSService) stampObject(ko *KvsObject, ex time.Duration) {
	created := time.Now()
	expires := created.Add(ex * time.Millisecond)

	ko.created = &created
	ko.expires = &expires

}
