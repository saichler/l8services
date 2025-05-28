package manager

import (
	"errors"
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceManager) DeActivate(serviceName string, serviceArea uint16, r ifs.IResources, l ifs.IServiceCacheListener) error {

	if serviceName == "" {
		return errors.New("Service name is empty")
	}

	handler, ok := this.services.del(serviceName, serviceArea)
	if !ok {
		return errors.New("Can't find service " + serviceName)
	}

	defer handler.DeActivate()

	ifs.RemoveService(this.resources.SysConfig().Services, serviceName, int32(serviceArea))
	vnic, ok := l.(ifs.IVNic)
	if ok {
		vnic.NotifyServiceRemoved(serviceName, serviceArea)
	}
	return nil
}
