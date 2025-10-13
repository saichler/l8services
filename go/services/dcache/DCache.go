package dcache

import (
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8utils/go/utils/cache"
)

type DCache struct {
	cache     *cache.Cache
	listener  ifs.IServiceCacheListener
	resources ifs.IResources
}

func NewDistributedCache(serviceName string, serviceArea byte, sample interface{}, initElements []interface{},
	listener ifs.IServiceCacheListener, resources ifs.IResources) ifs.IDistributedCache {
	return NewDistributedCacheWithStorage(serviceName, serviceArea, sample, initElements, listener, resources, nil, false)
}

func NewDistributedCacheNoSync(serviceName string, serviceArea byte, sample interface{}, initElements []interface{},
	listener ifs.IServiceCacheListener, resources ifs.IResources) ifs.IDistributedCache {
	return NewDistributedCacheWithStorage(serviceName, serviceArea, sample, initElements, listener, resources, nil, true)
}

func NewDistributedCacheWithStorage(serviceName string, serviceArea byte, sample interface{}, initElements []interface{},
	listener ifs.IServiceCacheListener, resources ifs.IResources, store ifs.IStorage, noSync bool) ifs.IDistributedCache {
	this := &DCache{}
	this.cache = cache.NewCache(sample, initElements, store, resources)
	this.listener = listener
	this.resources = resources
	this.cache.SetNotificationsFor(serviceName, serviceArea)
	if listener != nil && !noSync {
		resources.Services().RegisterDistributedCache(this)
	}
	return this
}

func (this *DCache) ServiceName() string {
	return this.cache.ServiceName()
}

func (this *DCache) ServiceArea() byte {
	return this.cache.ServiceArea()
}

func (this *DCache) Size() int {
	return this.cache.Size()
}
