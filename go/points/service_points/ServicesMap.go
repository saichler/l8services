package service_points

import (
	"bytes"
	"github.com/saichler/shared/go/share/maps"
	"github.com/saichler/types/go/common"
	"strconv"
)

type ServicesMap struct {
	services *maps.SyncMap
}

func NewServicesMap() *ServicesMap {
	newMap := &ServicesMap{}
	newMap.services = maps.NewSyncMap()
	return newMap
}

func (mp *ServicesMap) put(serviceName string, serviceArea uint16, handler common.IServicePointHandler) {
	key := serviceKey(serviceName, serviceArea)
	mp.services.Put(key, handler)
}

func (mp *ServicesMap) get(serviceName string, serviceArea uint16) (common.IServicePointHandler, bool) {
	key := serviceKey(serviceName, serviceArea)
	value, ok := mp.services.Get(key)
	if value != nil {
		return value.(common.IServicePointHandler), ok
	}
	return nil, ok
}

func (mp *ServicesMap) contains(serviceName string, serviceArea uint16) bool {
	key := serviceKey(serviceName, serviceArea)
	return mp.services.Contains(key)
}

func serviceKey(serviceName string, serviceArea uint16) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}
