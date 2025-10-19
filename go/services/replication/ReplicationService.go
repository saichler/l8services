package replication

import (
	"errors"

	"github.com/saichler/l8bus/go/overlay/protocol"
	"github.com/saichler/l8reflect/go/reflect/introspecting"
	"github.com/saichler/l8services/go/services/dcache"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8services"
)

const (
	ServiceType = "ReplicationService"
	ServiceName = "Replicas"
	ServiceArea = byte(0)
)

type ReplicationService struct {
	cache ifs.IDistributedCache
}

func (this *ReplicationService) Activate(sla *ifs.ServiceLevelAgreement, vnic ifs.IVNic) error {
	node, _ := vnic.Resources().Introspector().Inspect(&l8services.L8ReplicationIndex{})
	introspecting.AddPrimaryKeyDecorator(node, "ServiceName", "ServiceArea")
	this.cache = dcache.NewDistributedCache(sla.ServiceName(), sla.ServiceArea(), &l8services.L8ReplicationIndex{},
		nil, vnic, vnic.Resources())
	return nil
}

func (this *ReplicationService) DeActivate() error {
	return nil
}

func (this *ReplicationService) Post(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	for _, elem := range pb.Elements() {
		this.cache.Post(elem, pb.Notification())
	}
	return nil
}
func (this *ReplicationService) Put(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	for _, elem := range pb.Elements() {
		this.cache.Put(elem, pb.Notification())
	}
	return nil
}
func (this *ReplicationService) Patch(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	for _, elem := range pb.Elements() {
		this.cache.Patch(elem, pb.Notification())
	}
	return nil
}
func (this *ReplicationService) Delete(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	for _, elem := range pb.Elements() {
		this.cache.Delete(elem, pb.Notification())
	}
	return nil
}

func (this *ReplicationService) Get(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	return nil
}

func (this *ReplicationService) Failed(pb ifs.IElements, vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	return nil
}

func (this *ReplicationService) TransactionConfig() ifs.ITransactionConfig {
	return nil
}

func Service(r ifs.IResources) ifs.IServiceHandler {
	repService, _ := r.Services().ServiceHandler(ServiceName, ServiceArea)
	return repService
}

func ReplicationIndex(serviceName string, serviceArea byte, r ifs.IResources) *l8services.L8ReplicationIndex {
	repService, ok := r.Services().ServiceHandler(ServiceName, ServiceArea)
	if !ok {
		return nil
	}
	replicationService, ok := repService.(*ReplicationService)
	if !ok {
		return nil
	}

	filter := &l8services.L8ReplicationIndex{}
	filter.ServiceName = serviceName
	filter.ServiceArea = int32(serviceArea)

	index, err := replicationService.cache.Get(filter)
	if err == nil {
		return index.(*l8services.L8ReplicationIndex)
	}
	return nil
}

func (this *ReplicationService) WebService() ifs.IWebService {
	return nil
}
func (this *ReplicationService) Replication() bool {
	return false
}
func (this *ReplicationService) ReplicationCount() int {
	return 0
}
func (this *ReplicationService) KeyOf(pb ifs.IElements, r ifs.IResources) string {
	return ""
}
func (this *ReplicationService) ConcurrentGets() bool {
	return false
}

func ReplicationFor(msg *ifs.Message, r ifs.IResources, service ifs.IServiceHandler) (map[string]byte, error) {
	pb, err := protocol.ElementsOf(msg, r)
	if err != nil {
		return nil, err
	}
	key := service.TransactionConfig().KeyOf(pb, r)
	repIndex := ReplicationIndex(msg.ServiceName(), msg.ServiceArea(), r)
	if repIndex == nil {
		return nil, errors.New("Replication Index not found")
	}
	locations, ok := repIndex.Keys[key]
	result := map[string]byte{}
	if !ok {
		return result, nil
	}

	for uuid, rep := range locations.Location {
		result[uuid] = byte(rep)
	}
	return result, nil
}
