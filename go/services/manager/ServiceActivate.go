package manager

import (
	"errors"
	"reflect"
	"time"

	"github.com/saichler/l8services/go/services/replication"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8services"
	"github.com/saichler/l8types/go/types/l8system"
)

func (this *ServiceManager) Activate(sla *ifs.ServiceLevelAgreement, vnic ifs.IVNic) (ifs.IServiceHandler, error) {
	var handler ifs.IServiceHandler
	var ok bool
	var err error

	if vnic == nil {
		return handler, errors.New("Vnic cannot be nil when activating")
	}
	vnic.Resources().Logger().Debug("[Activate]", vnic.Resources().SysConfig().LocalUuid, "-",
		sla.ServiceName(), " ", sla.ServiceArea(), " ", vnic.Resources().SysConfig().LocalAlias)

	if sla.ServiceHandlerInstance() == nil {
		return handler, errors.New("SLA does not contain a service instance")
	}

	if sla.ServiceName() == "" {
		return nil, errors.New("SLA Service name is empty")
	}

	if len(sla.ServiceName()) > 10 {
		panic("SLA Service name " + sla.ServiceName() + " must be less than 10 characters long")
		return handler, errors.New("SLA Service name " + sla.ServiceName() + " must be less than 10 characters long")
	}

	handler, ok = this.services.get(sla.ServiceName(), sla.ServiceArea())
	if ok {
		return handler, nil
	}

	h := vnic.Resources().Registry().NewOf(sla.ServiceHandlerInstance())
	handler = h.(ifs.IServiceHandler)

	err = handler.Activate(sla, vnic)

	this.services.put(sla.ServiceName(), sla.ServiceArea(), handler)
	ifs.AddService(this.resources.SysConfig(), sla.ServiceName(), int32(sla.ServiceArea()))

	//Publish the serivce to all vnets
	this.publishService(sla, vnic)

	//Notify Health of service
	e := vnic.NotifyServiceAdded([]string{sla.ServiceName()}, sla.ServiceArea())
	if e != nil && err == nil {
		err = e
	}

	webService := handler.WebService()

	if ok && webService != nil {
		vnic.Resources().Logger().Debug("Sent Webservice multicast for ", sla.ServiceName(), " area ", sla.ServiceArea())
		vnic.Multicast(ifs.WebService, 0, ifs.POST, webService.Serialize())
	}

	e = this.registerForReplication(sla.ServiceName(), sla.ServiceArea(), handler, vnic)
	if e != nil && err == nil {
		err = e
	}

	// Only trigger election and participant registration for services with TransactionConfig
	this.triggerElections(sla.ServiceName(), sla.ServiceArea(), handler, vnic)

	return handler, err
}

func (this *ServiceManager) publishService(sla *ifs.ServiceLevelAgreement, vnic ifs.IVNic) {
	serviceData := &l8system.L8ServiceData{}
	serviceData.ServiceName = sla.ServiceName()
	serviceData.ServiceArea = int32(sla.ServiceArea())
	serviceData.ServiceUuid = this.resources.SysConfig().LocalUuid
	data := &l8system.L8SystemMessage_ServiceData{ServiceData: serviceData}
	sysmsg := &l8system.L8SystemMessage{Action: l8system.L8SystemAction_Service_Add, Data: data}
	sysmsg.Publish = true
	vnic.Multicast(ifs.SysMsg, ifs.SysArea, ifs.POST, sysmsg)
}

func (this *ServiceManager) registerForReplication(serviceName string, serviceArea byte, handler ifs.IServiceHandler, vnic ifs.IVNic) error {
	if handler.TransactionConfig() != nil && handler.TransactionConfig().Replication() {
		if handler.TransactionConfig().ReplicationCount() == 0 {
			return errors.New("Service Handler " + reflect.ValueOf(handler).Elem().Type().Name() + " has replication set to true with 0 replication count!")
		} else {
			repService := replication.Service(vnic.Resources())
			if repService == nil {
				sla := ifs.NewServiceLevelAgreement(&replication.ReplicationService{}, replication.ServiceName, replication.ServiceArea, true, nil)
				repService, _ = this.Activate(sla, vnic)
			}

			index := &l8services.L8ReplicationIndex{}
			index.ServiceName = serviceName
			index.ServiceArea = int32(serviceArea)
			index.Keys = make(map[string]*l8services.L8ReplicationKey)
			repService.Post(object.New(nil, index), vnic)
		}
	}
	return nil
}

func (this *ServiceManager) triggerElections(serviceName string, serviceArea byte, handler ifs.IServiceHandler, vnic ifs.IVNic) {
	// Only trigger election and participant registration for services with TransactionConfig
	if handler.TransactionConfig() != nil {
		// Register as participant for this service
		localUuid := this.resources.SysConfig().LocalUuid
		this.participantRegistry.RegisterParticipant(serviceName, serviceArea, localUuid)

		// Query for existing participants first
		vnic.Multicast(serviceName, serviceArea, ifs.ServiceQuery, nil)

		// Announce ourselves as a participant
		vnic.Multicast(serviceName, serviceArea, ifs.ServiceRegister, nil)

		// Send additional queries with delay to catch nodes that activated concurrently
		go func() {
			time.Sleep(100 * time.Millisecond)
			vnic.Multicast(serviceName, serviceArea, ifs.ServiceQuery, nil)

			time.Sleep(200 * time.Millisecond)
			vnic.Multicast(serviceName, serviceArea, ifs.ServiceQuery, nil)
		}()

		// Trigger election for this service
		this.leaderElection.StartElectionForService(serviceName, serviceArea, vnic)
	}
}
