package service_points

import (
	"bytes"
	"github.com/saichler/shared/go/share/maps"
	"github.com/saichler/types/go/common"
	"strconv"
)

type ServicesMap struct {
	impl *maps.SyncMap
}

func NewServicesMap() *ServicesMap {
	newMap := &ServicesMap{}
	newMap.impl = maps.NewSyncMap()
	return newMap
}

func (mp *ServicesMap) Put(serviceName string, serviceArea uint16, value common.IServicePointHandler) bool {
	key := ServiceKey(serviceName, serviceArea)
	return mp.impl.Put(key, value)
}

func (mp *ServicesMap) Get(serviceName string, serviceArea uint16) (common.IServicePointHandler, bool) {
	key := ServiceKey(serviceName, serviceArea)
	value, ok := mp.impl.Get(key)
	if value != nil {
		return value.(common.IServicePointHandler), ok
	}
	return nil, ok
}

func (mp *ServicesMap) Contains(serviceName string, serviceArea uint16) bool {
	key := ServiceKey(serviceName, serviceArea)
	return mp.impl.Contains(key)
}

/*
func (mp *ServicesMap) Topics() map[string]bool {
	tops := mp.impl.KeysAsList(reflect.TypeOf(""), nil).([]string)
	result := make(map[string]bool)
	for _, topic := range tops {
		result[topic] = true
	}
	return result
}*/

func ServiceKey(serviceName string, serviceArea uint16) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}
