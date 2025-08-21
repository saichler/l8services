package manager

import (
	"errors"

	"github.com/saichler/l8services/go/services/replication"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types"
)

func (this *ServiceManager) Activate(typeName string, serviceName string, serviceArea byte,
	r ifs.IResources, l ifs.IServiceCacheListener, args ...interface{}) (ifs.IServiceHandler, error) {

	if typeName == "" {
		return nil, errors.New("typeName is empty")
	}

	if serviceName == "" {
		return nil, errors.New("Service name is empty")
	}

	if len(serviceName) > 10 {
		panic("Service name " + serviceName + " must be less than 10 characters long")
		return nil, errors.New("Service name " + serviceName + " must be less than 10 characters long")
	}

	handler, ok := this.services.get(serviceName, serviceArea)
	if ok {
		return handler, nil
	}

	info, err := this.resources.Registry().Info(typeName)
	if err != nil {
		return nil, errors.New("Activate: " + err.Error())
	}
	h, err := info.NewInstance()
	if err != nil {
		return nil, errors.New("Activate: " + err.Error())
	}
	handler = h.(ifs.IServiceHandler)
	err = handler.Activate(serviceName, serviceArea, r, l, args...)
	if err != nil {
		return nil, errors.New("Activate: " + err.Error())
	}
	this.services.put(serviceName, serviceArea, handler)
	ifs.AddService(this.resources.SysConfig(), serviceName, int32(serviceArea))
	vnic, ok := l.(ifs.IVNic)

	if ok {
		serviceData := &types.ServiceData{}
		serviceData.ServiceName = serviceName
		serviceData.ServiceArea = int32(serviceArea)
		serviceData.ServiceUuid = this.resources.SysConfig().LocalUuid
		data := &types.SystemMessage_ServiceData{ServiceData: serviceData}
		sysmsg := &types.SystemMessage{Action: types.SystemAction_Service_Add, Data: data}
		vnic.Multicast(ifs.SysMsg, ifs.SysArea, ifs.POST, sysmsg)
	}

	serviceNames := []string{serviceName}

	if handler.TransactionMethod() != nil && handler.TransactionMethod().Replication() {
		if handler.TransactionMethod().ReplicationCount() == 0 {
			r.Logger().Error("Service point ", typeName, " has replication set to true with 0 replication count!")
		} else {
			repServiceName := replication.ReplicationNameOf(serviceName)
			serviceNames = append(serviceNames, repServiceName)
			this.RegisterServiceHandlerType(&replication.ReplicationService{})
			_, err = this.Activate(replication.ServiceType, repServiceName, serviceArea, r, l)
			if err != nil {
				return nil, err
			}
		}
	}

	if ok && typeName != replication.ServiceType {
		err = vnic.NotifyServiceAdded(serviceNames, serviceArea)
	}

	webService := handler.WebService()

	if ok && webService != nil {
		vnic.Resources().Logger().Info("Sent Webservice multicast for ", serviceName, " area ", serviceArea)
		vnic.Multicast(ifs.WebService, 0, ifs.POST, webService.Serialize())
	}

	return handler, err
}
