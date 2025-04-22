package replication

import (
	"bytes"
	"github.com/saichler/reflect/go/reflect/introspecting"
	"github.com/saichler/servicepoints/go/points/dcache"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"time"
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
	serviceName = nameOf(serviceName)
	node, _ := resources.Introspector().Inspect(&types.ReplicationIndex{})
	introspecting.AddPrimaryKeyDecorator(node, "ServiceName")
	uuid := resources.SysConfig().LocalUuid
	this.cache = dcache.NewDistributedCache(serviceName, serviceArea, "ReplicationIndex",
		uuid, listener, resources.Introspector())
	index := &types.ReplicationIndex{}
	index.ServiceName = serviceName
	index.ServiceArea = int32(serviceArea)
	index.Keys = make(map[string]*types.ReplicationKey)
	index.EndPoints = make(map[string]*types.ReplicationEndPoint)
	index.EndPoints[uuid] = &types.ReplicationEndPoint{Score: 1}
	return nil
}

func nameOf(serviceName string) string {
	buff := bytes.Buffer{}
	buff.WriteString(Prefix)
	buff.WriteString(serviceName)
	return buff.String()
}

func (this *ReplicationServicePoint) addEndPoint(index *types.ReplicationIndex) {
	time.Sleep(time.Second)
	this.cache.Put(index.ServiceName, index)
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
