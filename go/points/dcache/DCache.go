package dcache

import (
	"errors"
	"github.com/saichler/reflect/go/reflect/cloning"
	"github.com/saichler/reflect/go/reflect/updating"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"sync"
)

type DCache struct {
	cache        map[string]interface{}
	mtx          *sync.RWMutex
	cond         *sync.Cond
	listener     common.IServicePointCacheListener
	cloner       *cloning.Cloner
	introspector common.IIntrospector
	source       string
	serviceName  string
	serviceArea  uint16
	modelType    string
	sequence     uint32
}

func NewDistributedCache(serviceName string, serviceArea uint16, modelType, source string,
	listener common.IServicePointCacheListener, introspector common.IIntrospector) common.IDistributedCache {
	this := &DCache{}
	this.cache = make(map[string]interface{})
	this.mtx = &sync.RWMutex{}
	this.cond = sync.NewCond(this.mtx)
	this.listener = listener
	this.cloner = cloning.NewCloner()
	this.introspector = introspector
	this.source = source
	this.serviceName = serviceName
	this.serviceArea = serviceArea
	this.modelType = modelType
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

func (this *DCache) Put(k string, v interface{}, sourceNotification ...bool) (*types.NotificationSet, error) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	var n *types.NotificationSet
	var e error
	isNotification := (sourceNotification != nil && len(sourceNotification) > 0 && sourceNotification[0])

	item, ok := this.cache[k]
	//If the item does not exist in the cache
	if !ok {
		//First clone the value so we can use it in the notification.
		itemClone := this.cloner.Clone(v)
		//Place the value in the cache
		this.cache[k] = v
		//Send the notification using the clone outside the current go routine
		if this.listener != nil && !isNotification {
			n, e = this.createAddNotification(itemClone, k)
			if e != nil {
				return n, e
			}
			go this.listener.PropertyChangeNotification(n)
		}
		return n, e
	}
	//Place the value in the cache
	this.cache[k] = v

	//if the source is a notification, don't send notification
	if isNotification {
		return n, e
	}

	//Clone the existing item
	itemClone := this.cloner.Clone(item)

	//Create a new updater
	putUpdater := updating.NewUpdater(this.introspector, true)

	//update the item clone with the new element where nil is valid
	e = putUpdater.Update(itemClone, v)
	if e != nil {
		return n, e
	}

	//if there are changes, then nothing to do
	changes := putUpdater.Changes()
	if changes == nil || len(changes) == 0 {
		return nil, nil
	}

	if this.listener != nil {
		n, e = this.createReplaceNotification(item, v, k)
		if e != nil {
			return n, e
		}
		go this.listener.PropertyChangeNotification(n)
	}

	return n, e
}

func (this *DCache) Update(k string, v interface{}, sourceNotification ...bool) (*types.NotificationSet, error) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	var n *types.NotificationSet
	var e error
	isNotification := (sourceNotification != nil && len(sourceNotification) > 0 && sourceNotification[0])

	item, ok := this.cache[k]
	//If the item does not exist in the cache
	if !ok {
		//First clone the value so we can use it in the notification.
		itemClone := this.cloner.Clone(v)
		//Place the value in the cache
		this.cache[k] = v
		//Send the notification using the clone outside the current go routine
		if this.listener != nil && !isNotification {
			n, e = this.createAddNotification(itemClone, k)
			if e != nil {
				return n, e
			}
			go this.listener.PropertyChangeNotification(n)
		}
		return n, e
	}
	//Clone the existing item
	itemClone := this.cloner.Clone(item)
	//Create a new updater
	patchUpdater := updating.NewUpdater(this.introspector, false)
	//update the item clone with the new element where nil is valid
	e = patchUpdater.Update(itemClone, v)
	if e != nil {
		return n, e
	}

	//if there are changes, then nothing to do
	changes := patchUpdater.Changes()
	if changes == nil {
		return nil, nil
	}

	//Apply the changes to the existing item
	for _, change := range changes {
		change.Apply(item)
	}

	//if the source is notification, don't send notification
	if isNotification {
		return n, e
	}

	n, e = this.createUpdateNotification(changes, k)
	if e != nil {
		return n, e
	}

	if this.listener != nil {
		go this.listener.PropertyChangeNotification(n)
	}

	return n, e
}

func (this *DCache) Delete(k string, sourceNotification ...bool) (*types.NotificationSet, error) {
	this.mtx.Lock()
	defer this.mtx.Unlock()

	var n *types.NotificationSet
	var e error
	isNotification := (sourceNotification != nil && len(sourceNotification) > 0 && sourceNotification[0])

	item, ok := this.cache[k]

	if !ok {
		return nil, errors.New("Key " + k + " not found")
	}
	delete(this.cache, k)

	if this.listener != nil && !isNotification {
		n, e = this.createDeleteNotification(item, k)
		if e != nil {
			return n, e
		}
		go this.listener.PropertyChangeNotification(n)
	}
	return n, nil
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
