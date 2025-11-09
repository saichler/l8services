package dcache

import (
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8notify"
	"github.com/saichler/l8utils/go/utils/cache"
	"github.com/saichler/l8utils/go/utils/queues"
)

type DCache struct {
	cache     *cache.Cache
	listener  ifs.IServiceCacheListener
	resources ifs.IResources
	nQueue    *queues.Queue
	running   bool
}

func NewDistributedCache(serviceName string, serviceArea byte, sample interface{}, initElements []interface{},
	listener ifs.IServiceCacheListener, resources ifs.IResources) ifs.IDistributedCache {
	return NewDistributedCacheWithStorage(serviceName, serviceArea, sample, initElements, listener, resources, nil)
}

func NewDistributedCacheNoSync(serviceName string, serviceArea byte, sample interface{}, initElements []interface{},
	listener ifs.IServiceCacheListener, resources ifs.IResources) ifs.IDistributedCache {
	return NewDistributedCacheWithStorage(serviceName, serviceArea, sample, initElements, listener, resources, nil)
}

func NewDistributedCacheWithStorage(serviceName string, serviceArea byte, sample interface{}, initElements []interface{},
	listener ifs.IServiceCacheListener, resources ifs.IResources, store ifs.IStorage) ifs.IDistributedCache {
	this := &DCache{}
	this.cache = cache.NewCache(sample, initElements, store, resources)
	this.listener = listener
	this.resources = resources
	this.cache.SetNotificationsFor(serviceName, serviceArea)
	this.nQueue = queues.NewQueue("Nitifiction Queue", 50000)
	this.running = true
	if this.listener != nil {
		go this.processNotificationQueue()
	}
	return this
}

func (this *DCache) processNotificationQueue() {
	for this.running {
		set, ok := this.nQueue.Next().(*l8notify.L8NotificationSet)
		if ok {
			this.listener.PropertyChangeNotification(set)
		}
	}
}

func (this *DCache) Shutdown() {
	this.running = false
	this.nQueue.Add(nil)
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

func (this *DCache) Cache() *cache.Cache {
	return this.cache
}
