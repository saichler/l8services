package service_points

import (
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/share/maps"
	"reflect"
)

type String2ServicePointMap struct {
	impl *maps.SyncMap
}

func NewString2ServicePointMap() *String2ServicePointMap {
	newMap := &String2ServicePointMap{}
	newMap.impl = maps.NewSyncMap()
	return newMap
}

func (mp *String2ServicePointMap) Put(key string, value interfaces.IServicePointHandler) bool {
	return mp.impl.Put(key, value)
}

func (mp *String2ServicePointMap) Get(key string) (interfaces.IServicePointHandler, bool) {
	value, ok := mp.impl.Get(key)
	if value != nil {
		return value.(interfaces.IServicePointHandler), ok
	}
	return nil, ok
}

func (mp *String2ServicePointMap) Contains(key string) bool {
	return mp.impl.Contains(key)
}

func (mp *String2ServicePointMap) Topics() map[string]bool {
	tops := mp.impl.KeysAsList(reflect.TypeOf(""), nil).([]string)
	result := make(map[string]bool)
	for _, topic := range tops {
		result[topic] = true
	}
	return result
}
