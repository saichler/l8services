package manager

import (
	"errors"
	"fmt"

	"github.com/saichler/l8services/go/services/replication"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8system"
)

func (this *ServiceManager) Activate(typeName string, serviceName string, serviceArea byte,
	r ifs.IResources, l ifs.IServiceCacheListener, args ...interface{}) (ifs.IServiceHandler, error) {
	r.Logger().Debug("[Activate]", r.SysConfig().LocalUuid, "-", serviceName, "-", serviceArea, "-", l != nil)
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
		serviceData := &l8system.L8ServiceData{}
		serviceData.ServiceName = serviceName
		serviceData.ServiceArea = int32(serviceArea)
		serviceData.ServiceUuid = this.resources.SysConfig().LocalUuid
		data := &l8system.L8SystemMessage_ServiceData{ServiceData: serviceData}
		sysmsg := &l8system.L8SystemMessage{Action: l8system.L8SystemAction_Service_Add, Data: data}
		sysmsg.Publish = true
		vnic.Multicast(ifs.SysMsg, ifs.SysArea, ifs.POST, sysmsg)
	}

	serviceNames := []string{serviceName}

	if handler.TransactionConfig() != nil && handler.TransactionConfig().Replication() {
		if handler.TransactionConfig().ReplicationCount() == 0 {
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

	// Only trigger election and participant registration for services with TransactionConfig
	if ok && handler.TransactionConfig() != nil {
		// Register as participant for this service
		localUuid := this.resources.SysConfig().LocalUuid
		fmt.Println("[ACTIVATE]", localUuid, "- Registering participant for", serviceName, "area", serviceArea)
		this.participantRegistry.RegisterParticipant(serviceName, serviceArea, localUuid)
		fmt.Println("[ACTIVATE]", localUuid, "- Multicasting ServiceRegister for", serviceName, "area", serviceArea)
		vnic.Multicast(serviceName, serviceArea, ifs.ServiceRegister, nil)

		// Trigger election for this service
		fmt.Println("[ACTIVATE]", localUuid, "- Starting election for", serviceName, "area", serviceArea)
		this.leaderElection.StartElectionForService(serviceName, serviceArea, vnic)
	}

	return handler, err
}
