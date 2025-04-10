package service_points

import (
	"bytes"
	"errors"
	"github.com/saichler/shared/go/share/maps"
	"github.com/saichler/types/go/common"
	"strconv"
)

type ServicesMap struct {
	services *maps.SyncMap
	active   *maps.SyncMap
}

type ServicePointEntry struct {
	servicePoint common.IServicePointHandler
}

func NewServicesMap() *ServicesMap {
	newMap := &ServicesMap{}
	newMap.services = maps.NewSyncMap()
	newMap.active = maps.NewSyncMap()
	return newMap
}

func (mp *ServicesMap) put(serviceName string, value common.IServicePointHandler) bool {
	return mp.services.Put(serviceName, value)
}

func (mp *ServicesMap) activate(serviceName string, serviceArea uint16, handler common.IServicePointHandler) error {
	if handler != nil {
		key := serviceActiveKey(serviceName, serviceArea)
		mp.active.Put(key, handler)
		return nil
	}
	service, ok := mp.getService(serviceName)
	if !ok {
		return errors.New("No such service " + serviceName)
	}
	key := serviceActiveKey(serviceName, serviceArea)
	mp.active.Put(key, service)
	return nil
}

func (mp *ServicesMap) getService(serviceName string) (common.IServicePointHandler, bool) {
	value, ok := mp.services.Get(serviceName)
	if value != nil {
		return value.(common.IServicePointHandler), ok
	}
	return nil, ok
}

func (mp *ServicesMap) getActiveService(serviceName string, serviceArea uint16) (common.IServicePointHandler, bool) {
	key := serviceActiveKey(serviceName, serviceArea)
	value, ok := mp.active.Get(key)
	if value != nil {
		return value.(common.IServicePointHandler), ok
	}
	return nil, ok
}

func (mp *ServicesMap) containsService(serviceName string) bool {
	return mp.services.Contains(serviceName)
}

func serviceActiveKey(serviceName string, serviceArea uint16) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}
