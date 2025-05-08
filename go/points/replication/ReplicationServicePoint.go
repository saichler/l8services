package replication

import (
	"bytes"
	"github.com/saichler/reflect/go/reflect/introspecting"
	"github.com/saichler/servicepoints/go/points/dcache"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types"
)

const (
	ServicePointType = "ReplicationServicePoint"
	Prefix           = "Rep-"
)

type ReplicationServicePoint struct {
	cache ifs.IDistributedCache
}

func (this *ReplicationServicePoint) Activate(serviceName string, serviceArea uint16,
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

func NameOf(serviceName string) string {
	buff := bytes.Buffer{}
	buff.WriteString(Prefix)
	buff.WriteString(serviceName)
	return buff.String()
}

func (this *ReplicationServicePoint) DeActivate() error {
	return nil
}

func (this *ReplicationServicePoint) Post(pb ifs.IElements, resourcs ifs.IResources) ifs.IElements {
	return nil
}
func (this *ReplicationServicePoint) Put(pb ifs.IElements, resourcs ifs.IResources) ifs.IElements {
	return nil
}
func (this *ReplicationServicePoint) Patch(pb ifs.IElements, resourcs ifs.IResources) ifs.IElements {
	incoming := pb.Element().(*types.ReplicationIndex)
	resourcs.Logger().Trace("Updating index on ", resourcs.SysConfig().LocalAlias)
	_, e := this.cache.Update(incoming.ServiceName, incoming, pb.Notification())
	if e != nil {
		panic(e)
	}
	return nil
}
func (this *ReplicationServicePoint) Delete(pb ifs.IElements, resourcs ifs.IResources) ifs.IElements {
	return nil
}
func (this *ReplicationServicePoint) GetCopy(pb ifs.IElements, resourcs ifs.IResources) ifs.IElements {
	return nil
}
func (this *ReplicationServicePoint) Get(pb ifs.IElements, resourcs ifs.IResources) ifs.IElements {
	return nil
}
func (this *ReplicationServicePoint) Failed(pb ifs.IElements, resourcs ifs.IResources, msg ifs.IMessage) ifs.IElements {
	return nil
}

func (this *ReplicationServicePoint) TransactionMethod() ifs.ITransactionMethod {
	return nil
}

func ReplicationIndex(serviceName string, serviceArea uint16, resources ifs.IResources) (*types.ReplicationIndex, ifs.IServiceHandler) {
	serviceName = NameOf(serviceName)
	rp, ok := resources.Services().ServicePointHandler(serviceName, serviceArea)
	if ok {
		rsp := rp.(*ReplicationServicePoint)
		return rsp.cache.Get(serviceName).(*types.ReplicationIndex), rsp
	}
	return nil, nil
}

func UpdateIndex(sp ifs.IServiceHandler, index *types.ReplicationIndex) {
	sp.(*ReplicationServicePoint).cache.Update(index.ServiceName, index, false)
}
