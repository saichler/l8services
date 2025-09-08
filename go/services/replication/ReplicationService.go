package replication

import (
	"bytes"

	"github.com/saichler/l8services/go/services/dcache"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types"
	"github.com/saichler/reflect/go/reflect/introspecting"
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
	node, _ := resources.Introspector().Inspect(&types.ReplicationIndex{})
	introspecting.AddPrimaryKeyDecorator(node, "ServiceName")
	uuid := resources.SysConfig().LocalUuid
	this.cache = dcache.NewDistributedCache(serviceName, serviceArea, "ReplicationIndex",
		uuid, listener, resources)
	index := &types.ReplicationIndex{}
	index.ServiceName = serviceName
	index.ServiceArea = int32(serviceArea)
	index.Keys = make(map[string]*types.ReplicationKey)
	index.EndPoints = make(map[string]*types.ReplicationEndPoint)
	index.EndPoints[uuid] = &types.ReplicationEndPoint{Score: 1}
	this.cache.Put(serviceName, index, true)
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
	incoming := pb.Element().(*types.ReplicationIndex)
	vnic.Resources().Logger().Trace("Updating index on ", vnic.Resources().SysConfig().LocalAlias)
	_, e := this.cache.Patch(incoming.ServiceName, incoming, pb.Notification())
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

func ReplicationIndex(serviceName string, serviceArea byte, resources ifs.IResources) (*types.ReplicationIndex, ifs.IServiceHandler) {
	serviceName = ReplicationNameOf(serviceName)
	rp, ok := resources.Services().ServiceHandler(serviceName, serviceArea)
	if ok {
		rsp := rp.(*ReplicationService)
		return rsp.cache.Get(serviceName).(*types.ReplicationIndex), rsp
	}
	return nil, nil
}

func UpdateIndex(sp ifs.IServiceHandler, index *types.ReplicationIndex) {
	sp.(*ReplicationService).cache.Patch(index.ServiceName, index, false)
}

func (this *ReplicationService) WebService() ifs.IWebService {
	return nil
}
