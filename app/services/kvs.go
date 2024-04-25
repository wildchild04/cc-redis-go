package services

import "sync"

type Kvs interface {
	Set(k string, v []byte) bool
	Get(k string) ([]byte, bool)
}

type kvSService struct {
	store *sync.Map
}

func NewKvSService() Kvs {
	return &kvSService{&sync.Map{}}
}

func (kvs *kvSService) Set(key string, value []byte) bool {

	kvs.store.Store(key, value)

	return true
}

func (kvs *kvSService) Get(k string) ([]byte, bool) {

	anyV, ok := kvs.store.Load(k)
	v, ok := anyV.([]byte)

	return v, ok
}
