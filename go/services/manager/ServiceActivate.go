// © 2025 Sharon Aicler (saichler@gmail.com)
//
// Layer 8 Ecosystem is licensed under the Apache License, Version 2.0.
// You may obtain a copy of the License at:
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"errors"
	"fmt"
	"github.com/saichler/l8bus/go/overlay/health"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/saichler/l8services/go/services/replication"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8services"
	"github.com/saichler/l8types/go/types/l8system"
)

// Activate registers and initializes a service based on its SLA configuration.
// It validates the SLA, registers types, creates the handler instance, sets up
// decorators, publishes the service to the network, and triggers elections if needed.
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

	if sla.ServiceItemList() != nil {
		vnic.Resources().Registry().Register(sla.ServiceItemList())
	}

	if sla.ServiceItem() != nil {
		vnic.Resources().Introspector().Decorators().AddPrimaryKeyDecorator(sla.ServiceItem(), sla.PrimaryKeys()...)
		vnic.Resources().Introspector().Decorators().AddUniqueKeyDecorator(sla.ServiceItem(), sla.UniqueKeys()...)
		vnic.Resources().Introspector().Decorators().AddNonUniqueKeyDecorator(sla.ServiceItem(), sla.NonUniqueKeys()...)
		if sla.AlwaysOverwrite() != nil {
			for _, alwaysOverwriteAttr := range sla.AlwaysOverwrite() {
				e := vnic.Resources().Introspector().Decorators().AddAlwayOverwriteDecorator(alwaysOverwriteAttr)
				if e != nil {
					panic(e)
				}
			}
		}
	}

	handler, ok = this.services.get(sla.ServiceName(), sla.ServiceArea())
	if ok {
		return handler, nil
	}

	h := vnic.Resources().Registry().NewOf(sla.ServiceHandlerInstance())
	handler = h.(ifs.IServiceHandler)

	err = handler.Activate(sla, vnic)
	if err != nil {
		panic(err)
	}

	this.services.put(sla.ServiceName(), sla.ServiceArea(), handler)
	ifs.AddService(this.resources.SysConfig(), sla.ServiceName(), int32(sla.ServiceArea()))

	//Publish the serivce to all vnets
	this.publishService(sla.ServiceName(), sla.ServiceArea(), vnic)

	//Notify Health of service
	e := vnic.NotifyServiceAdded([]string{sla.ServiceName()}, sla.ServiceArea())
	if e != nil && err == nil {
		err = e
	}

	webService := handler.WebService()
	if webService == nil {
		webService = sla.WebService()
	}
	
	if ok && webService != nil {
		vnic.Resources().Logger().Debug("Sent Webservice multicast for ", sla.ServiceName(), " area ", sla.ServiceArea())
		vnic.Multicast(ifs.WebService, 0, ifs.POST, webService.Serialize())
	}

	e = this.registerForReplication(sla.ServiceName(), sla.ServiceArea(), handler, vnic)
	if e != nil && err == nil {
		err = e
	}

	if sla.Stateful() {
		// Only trigger election and participant registration for services with TransactionConfig
		this.triggerElections(sla.ServiceName(), sla.ServiceArea(), handler, vnic)
	}
	return handler, err
}

// publishService announces a service to all virtual networks and updates
// the health status with the current service list.
func (this *ServiceManager) publishService(serviceName string, serviceArea byte, vnic ifs.IVNic) {
	serviceData := &l8system.L8ServiceData{}
	serviceData.ServiceName = serviceName
	serviceData.ServiceArea = int32(serviceArea)
	serviceData.ServiceUuid = this.resources.SysConfig().LocalUuid
	data := &l8system.L8SystemMessage_ServiceData{ServiceData: serviceData}
	sysmsg := &l8system.L8SystemMessage{Action: l8system.L8SystemAction_Service_Add, Data: data}
	sysmsg.Publish = true
	vnic.Multicast(ifs.SysMsg, ifs.SysAreaPrimary, ifs.POST, sysmsg)

	hs := health.HealthOf(vnic.Resources().SysConfig().LocalUuid, vnic.Resources())
	if hs != nil {
		hs.Services = vnic.Resources().Services().Services()
		vnic.Multicast(health.ServiceName, 0, ifs.PATCH, hs)
	}
}

// registerForReplication sets up replication for services that have it enabled,
// creating the replication service if needed and initializing the replication index.
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

// triggerElections initiates participant registration and leader election for a service.
// For Map-Reduce services, it registers as a participant; for transactional services,
// it also starts the election process.
func (this *ServiceManager) triggerElections(serviceName string, serviceArea byte, handler ifs.IServiceHandler, vnic ifs.IVNic) {
	_, isMapReduceService := handler.(ifs.IMapReduceService)
	if isMapReduceService {
		fmt.Println("Map Reduce Service:", reflect.ValueOf(handler).Elem().Type().Name())
	}
	shouldTriggerParticipant := isMapReduceService || handler.TransactionConfig() != nil
	if shouldTriggerParticipant {
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
	}
	// Only trigger election and participant registration for services with TransactionConfig
	if handler.TransactionConfig() != nil {
		// Trigger election for this service
		this.leaderElection.StartElectionForService(serviceName, serviceArea, vnic)
	}
}

// TriggerElections re-triggers elections for all transactional and Map-Reduce services.
// Used for recovery scenarios or when re-establishing cluster coordination.
func (this *ServiceManager) TriggerElections(vnic ifs.IVNic) {
	services := map[string]ifs.IServiceHandler{}
	this.services.services.Range(func(key, value interface{}) bool {
		h := value.(ifs.IServiceHandler)
		if h.TransactionConfig() != nil {
			services[key.(string)] = h
		}
		_, ok := value.(ifs.IMapReduceService)
		if ok {
			services[key.(string)] = h
		}
		return true
	})
	for k, h := range services {
		serviceName, serviceArea := serviceNameArea(k)
		this.publishService(serviceName, serviceArea, vnic)
		time.Sleep(time.Second)
		this.triggerElections(serviceName, serviceArea, h, vnic)
	}
}

// serviceNameArea extracts the service name and area from a combined key string.
func serviceNameArea(key string) (string, byte) {
	index := strings.Index(key, "--")
	serviceName := key[:index]
	sArea := key[index+2:]
	i, _ := strconv.Atoi(sArea)
	return serviceName, byte(i)
}
