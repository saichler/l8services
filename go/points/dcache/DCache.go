package dcache

import (
	"github.com/saichler/reflect/go/reflect/cloning"
	"github.com/saichler/types/go/common"
	"sync"
)

type DCache struct {
	cache       map[string]interface{}
	mtx         *sync.RWMutex
	cond        *sync.Cond
	listener    common.IServicePointCacheListener
	cloner      *cloning.Cloner
	resources   common.IResources
	source      string
	serviceName string
	serviceArea uint16
	modelType   string
	sequence    uint32
}

func NewDistributedCache(serviceName string, serviceArea uint16, modelType, source string,
	listener common.IServicePointCacheListener, resources common.IResources) common.IDistributedCache {
	this := &DCache{}
	this.cache = make(map[string]interface{})
	this.mtx = &sync.RWMutex{}
	this.cond = sync.NewCond(this.mtx)
	this.listener = listener
	this.cloner = cloning.NewCloner()
	this.resources = resources
	this.source = source
	this.serviceName = serviceName
	this.serviceArea = serviceArea
	this.modelType = modelType
	if listener != nil {
		resources.ServicePoints().RegisterDistributedCache(this)
	}
	return this
}

func (this *DCache) Get(k string) interface{} {
	this.mtx.RLock()
	defer this.mtx.RUnlock()
	item, ok := this.cache[k]
	if ok {
		itemClone := this.cloner.Clone(item)
		return itemClone
	}
	return nil
}

func (this *DCache) Collect(f func(interface{}) (bool, interface{})) map[string]interface{} {
	result := map[string]interface{}{}
	this.mtx.RLock()
	defer this.mtx.RUnlock()
	for k, v := range this.cache {
		ok, elem := f(v)
		if ok {
			result[k] = elem
		}
	}
	return result
}

func (this *DCache) ServiceName() string {
	return this.serviceName
}

func (this *DCache) ServiceArea() uint16 {
	return this.serviceArea
}
