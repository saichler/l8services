package dcache

import (
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/reflect/go/reflect/cloning"
	"sync"
)

type DCache struct {
	cache       map[string]interface{}
	mtx         *sync.RWMutex
	cond        *sync.Cond
	listener    ifs.IServiceCacheListener
	cloner      *cloning.Cloner
	resources   ifs.IResources
	source      string
	serviceName string
	serviceArea byte
	modelType   string
	sequence    uint32
	store       ifs.IStorage
}

func NewDistributedCache(serviceName string, serviceArea byte, modelType, source string,
	listener ifs.IServiceCacheListener, resources ifs.IResources) ifs.IDistributedCache {
	return NewDistributedCacheWithStorage(serviceName, serviceArea, modelType, source, listener, resources, nil)
}

func NewDistributedCacheWithStorage(serviceName string, serviceArea byte, modelType, source string,
	listener ifs.IServiceCacheListener, resources ifs.IResources, store ifs.IStorage) ifs.IDistributedCache {
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
	this.store = store
	if listener != nil {
		resources.Services().RegisterDistributedCache(this)
	}
	if this.store != nil {
		items := this.store.Collect(all)
		for k, v := range items {
			this.cache[k] = v
		}
	}
	return this
}

func (this *DCache) ServiceName() string {
	return this.serviceName
}

func (this *DCache) ServiceArea() byte {
	return this.serviceArea
}

func (this *DCache) cacheEnabled() bool {
	if this.store == nil {
		return true
	}
	return this.store.CacheEnabled()
}
