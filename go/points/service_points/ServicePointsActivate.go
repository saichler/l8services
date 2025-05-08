package service_points

import (
	"errors"
	"github.com/saichler/servicepoints/go/points/replication"
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServicePointsImpl) Activate(typeName string, serviceName string, serviceArea uint16,
	r ifs.IResources, l ifs.IServicePointCacheListener, args ...interface{}) (ifs.IServicePointHandler, error) {

	if typeName == "" {
		return nil, errors.New("typeName is empty")
	}

	if serviceName == "" {
		return nil, errors.New("Service name is empty")
	}

	handler, ok := this.services.get(serviceName, serviceArea)
	if ok {
		return handler, nil
	}

	info, err := this.introspector.Registry().Info(typeName)
	if err != nil {
		return nil, errors.New("Activate: " + err.Error())
	}
	h, err := info.NewInstance()
	if err != nil {
		return nil, errors.New("Activate: " + err.Error())
	}
	handler = h.(ifs.IServicePointHandler)
	err = handler.Activate(serviceName, serviceArea, r, l, args...)
	if err != nil {
		return nil, errors.New("Activate: " + err.Error())
	}
	this.services.put(serviceName, serviceArea, handler)
	ifs.AddService(this.config, serviceName, int32(serviceArea))
	vnic, ok := l.(ifs.IVirtualNetworkInterface)

	serviceNames := []string{serviceName}

	if handler.TransactionMethod() != nil && handler.TransactionMethod().Replication() {
		if handler.TransactionMethod().ReplicationCount() == 0 {
			r.Logger().Error("Service point ", typeName, " has replication set to true with 0 replication count!")
		} else {
			repServiceName := replication.NameOf(serviceName)
			serviceNames = append(serviceNames, repServiceName)
			this.AddServicePointType(&replication.ReplicationServicePoint{})
			_, err = this.Activate(replication.ServicePointType, repServiceName, serviceArea, r, l)
			if err != nil {
				return nil, err
			}
		}
	}

	if ok && typeName != replication.ServicePointType {
		err = vnic.NotifyServiceAdded(serviceNames, serviceArea)
	}
	return handler, err
}
