package manager

import (
	"bytes"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8utils/go/utils/maps"
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

func (mp *ServicesMap) put(serviceName string, serviceArea uint16, handler ifs.IServiceHandler) {
	key := serviceKey(serviceName, serviceArea)
	mp.services.Put(key, handler)
}

func (mp *ServicesMap) get(serviceName string, serviceArea uint16) (ifs.IServiceHandler, bool) {
	key := serviceKey(serviceName, serviceArea)
	value, ok := mp.services.Get(key)
	if value != nil {
		return value.(ifs.IServiceHandler), ok
	}
	return nil, ok
}

func (mp *ServicesMap) del(serviceName string, serviceArea uint16) (ifs.IServiceHandler, bool) {
	key := serviceKey(serviceName, serviceArea)
	value, ok := mp.services.Delete(key)
	if value != nil {
		return value.(ifs.IServiceHandler), ok
	}
	return nil, ok
}

func (mp *ServicesMap) contains(serviceName string, serviceArea uint16) bool {
	key := serviceKey(serviceName, serviceArea)
	return mp.services.Contains(key)
}

func (mp *ServicesMap) webServices() []ifs.IWebService {
	result := make([]ifs.IWebService, 0)
	mp.services.Iterate(func(key interface{}, value interface{}) {
		svc := value.(ifs.IServiceHandler)
		if svc.WebService() != nil {
			result = append(result, svc.WebService())
		}
	})
	return result
}

func serviceKey(serviceName string, serviceArea uint16) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}
