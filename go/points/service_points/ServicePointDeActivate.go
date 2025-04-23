package service_points

import (
	"errors"
	"github.com/saichler/types/go/common"
)

func (this *ServicePointsImpl) DeActivate(serviceName string, serviceArea uint16, r common.IResources, l common.IServicePointCacheListener) error {

	if serviceName == "" {
		return errors.New("Service name is empty")
	}

	handler, ok := this.services.del(serviceName, serviceArea)
	if !ok {
		return errors.New("Can't find service " + serviceName)
	}

	defer handler.DeActivate()

	common.RemoveService(this.config.Services, serviceName, int32(serviceArea))
	vnic, ok := l.(common.IVirtualNetworkInterface)
	if ok {
		vnic.NotifyServiceRemoved(serviceName, serviceArea)
	}
	return nil
}
