package replication

import (
	"bytes"
	"github.com/saichler/reflect/go/reflect/introspecting"
	"github.com/saichler/servicepoints/go/points/dcache"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
)

const (
	ServicePointType = "ReplicationServicePoint"
	Prefix           = "Rep-"
)

type ReplicationServicePoint struct {
	cache common.IDistributedCache
}

func (this *ReplicationServicePoint) Activate(serviceName string, serviceArea uint16,
	resources common.IResources, listener common.IServicePointCacheListener, args ...interface{}) error {
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

func NameOf(serviceName string) string {
	buff := bytes.Buffer{}
	buff.WriteString(Prefix)
	buff.WriteString(serviceName)
	return buff.String()
}

func (this *ReplicationServicePoint) DeActivate() error {
	return nil
}

func (this *ReplicationServicePoint) Post(pb common.IElements, resourcs common.IResources) common.IElements {
	return nil
}
func (this *ReplicationServicePoint) Put(pb common.IElements, resourcs common.IResources) common.IElements {
	return nil
}
func (this *ReplicationServicePoint) Patch(pb common.IElements, resourcs common.IResources) common.IElements {
	incoming := pb.Element().(*types.ReplicationIndex)
	this.cache.Update(incoming.ServiceName, incoming, pb.Notification())
	return nil
}
func (this *ReplicationServicePoint) Delete(pb common.IElements, resourcs common.IResources) common.IElements {
	return nil
}
func (this *ReplicationServicePoint) GetCopy(pb common.IElements, resourcs common.IResources) common.IElements {
	return nil
}
func (this *ReplicationServicePoint) Get(pb common.IElements, resourcs common.IResources) common.IElements {
	return nil
}
func (this *ReplicationServicePoint) Failed(pb common.IElements, resourcs common.IResources, msg common.IMessage) common.IElements {
	return nil
}

func (this *ReplicationServicePoint) TransactionMethod() common.ITransactionMethod {
	return nil
}

func ReplicationIndex(serviceName string, serviceArea uint16, resources common.IResources) (*types.ReplicationIndex, common.IServicePointHandler) {
	serviceName = NameOf(serviceName)
	rp, ok := resources.ServicePoints().ServicePointHandler(serviceName, serviceArea)
	if ok {
		rsp := rp.(*ReplicationServicePoint)
		return rsp.cache.Get(serviceName).(*types.ReplicationIndex), rsp
	}
	return nil, nil
}

func UpdateIndex(sp common.IServicePointHandler, index *types.ReplicationIndex) {
	sp.(*ReplicationServicePoint).cache.Update(index.ServiceName, index, false)
}
