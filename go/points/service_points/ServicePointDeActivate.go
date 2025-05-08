package service_points

import (
	"errors"
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServicePointsImpl) DeActivate(serviceName string, serviceArea uint16, r ifs.IResources, l ifs.IServicePointCacheListener) error {

	if serviceName == "" {
		return errors.New("Service name is empty")
	}

	handler, ok := this.services.del(serviceName, serviceArea)
	if !ok {
		return errors.New("Can't find service " + serviceName)
	}

	defer handler.DeActivate()

	ifs.RemoveService(this.config.Services, serviceName, int32(serviceArea))
	vnic, ok := l.(ifs.IVirtualNetworkInterface)
	if ok {
		vnic.NotifyServiceRemoved(serviceName, serviceArea)
	}
	return nil
}
