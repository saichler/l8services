package cache

import (
	"errors"
	"fmt"
	"github.com/saichler/reflect/go/reflect/cloning"
	"github.com/saichler/reflect/go/reflect/updating"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"sync"
)

type Cache struct {
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

func NewModelCache(serviceName string, serviceArea uint16, modelType, source string,
	listener common.IServicePointCacheListener, introspector common.IIntrospector) *Cache {
	this := &Cache{}
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

func (this *Cache) Get(k string) interface{} {
	this.mtx.RLock()
	defer this.mtx.RUnlock()
	item, ok := this.cache[k]
	if ok {
		itemClone := this.cloner.Clone(item)
		return itemClone
	}
	return nil
}

func (this *Cache) Put(k string, v interface{}) (*types.NotificationSet, error) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	var n *types.NotificationSet
	var e error

	item, ok := this.cache[k]
	//If the item does not exist in the cache
	if !ok {
		//First clone the value so we can use it in the notification.
		itemClone := this.cloner.Clone(v)
		//Place the value in the cache
		this.cache[k] = v
		//Send the notification using the clone outside the current go routine
		if this.listener != nil {
			n, e = this.createAddNotification(itemClone)
			if e != nil {
				return n, e
			}
			go this.listener.PropertyChangeNotification(n)
		}
		return n, e
	}
	//Place the value in the cache
	this.cache[k] = v

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

	for _, chg := range changes {
		fmt.Println(chg.PropertyId(), " old:", chg.OldValue(), " new:", chg.NewValue())
	}

	if this.listener != nil {
		n, e = this.createReplaceNotification(item, v)
		if e != nil {
			return n, e
		}
		go this.listener.PropertyChangeNotification(n)
	}

	return n, e
}

func (this *Cache) Update(k string, v interface{}) (*types.NotificationSet, error) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	var n *types.NotificationSet
	var e error

	item, ok := this.cache[k]
	//If the item does not exist in the cache
	if !ok {
		return n, errors.New("Key " + k + " not found")
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

	n, e = this.createUpdateNotification(changes)
	if e != nil {
		return n, e
	}

	if this.listener != nil {
		go this.listener.PropertyChangeNotification(n)
	}
	return n, e
}

func (this *Cache) Delete(k string) error {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	item, ok := this.cache[k]
	if !ok {
		return errors.New("Key " + k + " not found")
	}
	delete(this.cache, k)
	if this.listener != nil {
		n, e := this.createDeleteNotification(item)
		if e != nil {
			return e
		}
		go this.listener.PropertyChangeNotification(n)
	}
	return nil
}

func (this *Cache) Collect(f func(interface{}) (bool, interface{})) map[string]interface{} {
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
