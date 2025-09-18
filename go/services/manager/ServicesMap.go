package manager

import (
	"bytes"
	"strconv"
	"strings"
	"sync"

	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8services"
)

type ServicesMap struct {
	services *sync.Map
}

func NewServicesMap() *ServicesMap {
	newMap := &ServicesMap{}
	newMap.services = &sync.Map{}
	return newMap
}

func (mp *ServicesMap) put(serviceName string, serviceArea byte, handler ifs.IServiceHandler) {
	key := serviceKey(serviceName, serviceArea)
	mp.services.Store(key, handler)
}

func (mp *ServicesMap) get(serviceName string, serviceArea byte) (ifs.IServiceHandler, bool) {
	key := serviceKey(serviceName, serviceArea)
	value, ok := mp.services.Load(key)
	if value != nil {
		return value.(ifs.IServiceHandler), ok
	}
	return nil, ok
}

func (mp *ServicesMap) del(serviceName string, serviceArea byte) (ifs.IServiceHandler, bool) {
	key := serviceKey(serviceName, serviceArea)
	value, ok := mp.services.LoadAndDelete(key)
	if value != nil {
		return value.(ifs.IServiceHandler), ok
	}
	return nil, ok
}

func (mp *ServicesMap) contains(serviceName string, serviceArea byte) bool {
	key := serviceKey(serviceName, serviceArea)
	_, ok := mp.services.Load(key)
	return ok
}

func (mp *ServicesMap) webServices() []ifs.IWebService {
	result := make([]ifs.IWebService, 0)
	mp.services.Range(func(key, value interface{}) bool {
		svc := value.(ifs.IServiceHandler)
		if svc.WebService() != nil {
			result = append(result, svc.WebService())
		}
		return true
	})
	return result
}

func serviceKey(serviceName string, serviceArea byte) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString("--")
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}

func (mp *ServicesMap) serviceList() *l8services.L8Services {
	s := &l8services.L8Services{}
	s.ServiceToAreas = make(map[string]*l8services.L8ServiceAreas)
	mp.services.Range(func(key, value interface{}) bool {
		str := key.(string)
		index := strings.Index(str, "--")
		name := str[0:index]
		areaStr := str[index+2:]
		areaInt, _ := strconv.Atoi(areaStr)
		sv, ok := s.ServiceToAreas[name]
		if !ok {
			sv = &l8services.L8ServiceAreas{}
			sv.Areas = make(map[int32]bool)
			s.ServiceToAreas[name] = sv
		}
		sv.Areas[int32(areaInt)] = true
		return true
	})
	return s
}
