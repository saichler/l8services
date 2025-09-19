package replication

import (
	"bytes"

	"github.com/saichler/l8services/go/services/dcache"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8services"
	"github.com/saichler/l8reflect/go/reflect/introspecting"
)

const (
	ServiceType = "ReplicationService"
	Prefix      = "R-"
)

type ReplicationService struct {
	cache ifs.IDistributedCache
}

func (this *ReplicationService) Activate(serviceName string, serviceArea byte,
	resources ifs.IResources, listener ifs.IServiceCacheListener, args ...interface{}) error {
	node, _ := resources.Introspector().Inspect(&l8services.L8ReplicationIndex{})
	introspecting.AddPrimaryKeyDecorator(node, "ServiceName")
	uuid := resources.SysConfig().LocalUuid

	index := &l8services.L8ReplicationIndex{}
	index.ServiceName = serviceName
	index.ServiceArea = int32(serviceArea)
	index.Keys = make(map[string]*l8services.L8ReplicationKey)
	index.EndPoints = make(map[string]*l8services.L8ReplicationEndPoint)
	index.EndPoints[uuid] = &l8services.L8ReplicationEndPoint{Score: 1}

	this.cache = dcache.NewDistributedCache(serviceName, serviceArea, index, []interface{}{index},
		listener, resources)

	return nil
}

func ReplicationNameOf(serviceName string) string {
	buff := bytes.Buffer{}
	buff.WriteString(Prefix)
	if len(serviceName) >= 8 {
		buff.WriteString(serviceName[0:8])
	} else {
		buff.WriteString(serviceName)
	}
	return buff.String()
}

func (this *ReplicationService) DeActivate() error {
	return nil
}

func (this *ReplicationService) Post(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	return nil
}
func (this *ReplicationService) Put(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	return nil
}
func (this *ReplicationService) Patch(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	incoming := pb.Element().(*l8services.L8ReplicationIndex)
	vnic.Resources().Logger().Trace("Updating index on ", vnic.Resources().SysConfig().LocalAlias)
	_, e := this.cache.Patch(incoming, pb.Notification())
	if e != nil {
		panic(e)
	}
	return nil
}
func (this *ReplicationService) Delete(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	return nil
}
func (this *ReplicationService) GetCopy(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
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

func ReplicationIndex(serviceName string, serviceArea byte, resources ifs.IResources) (*l8services.L8ReplicationIndex, ifs.IServiceHandler) {
	serviceName = ReplicationNameOf(serviceName)
	rp, ok := resources.Services().ServiceHandler(serviceName, serviceArea)
	if ok {
		rsp := rp.(*ReplicationService)
		filter := &l8services.L8ReplicationIndex{}
		filter.ServiceName = serviceName
		index, err := rsp.cache.Get(filter)
		if err == nil {
			return index.(*l8services.L8ReplicationIndex), rsp
		}
	}
	return nil, nil
}

func UpdateIndex(sp ifs.IServiceHandler, index *l8services.L8ReplicationIndex) {
	sp.(*ReplicationService).cache.Patch(index, false)
}

func (this *ReplicationService) WebService() ifs.IWebService {
	return nil
}
