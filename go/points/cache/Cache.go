package cache

import (
	"errors"
	"github.com/saichler/reflect/go/reflect/clone"
	"github.com/saichler/reflect/go/reflect/common"
	"github.com/saichler/reflect/go/reflect/updater"
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/shared/go/types"
	"sync"
)

type Cache struct {
	cache        map[string]interface{}
	mtx          *sync.RWMutex
	cond         *sync.Cond
	listener     ICacheListener
	cloner       *clone.Cloner
	introspector common.IIntrospect
	source       string
	typeName     string
	sequence     uint32
}

type ICacheListener interface {
	ModelItemAdded(interface{})
	ModelItemDeleted(interface{})
	PropertyChangeNotification(notify *types.NotificationSet)
}

func NewModelCache(typeName, source string, listener ICacheListener, introspector common.IIntrospect) *Cache {
	this := &Cache{}
	this.cache = make(map[string]interface{})
	this.mtx = &sync.RWMutex{}
	this.cond = sync.NewCond(this.mtx)
	this.listener = listener
	this.cloner = clone.NewCloner()
	this.introspector = introspector
	this.typeName = typeName
	this.source = source
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

func (this *Cache) Put(k string, v interface{}) error {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	item, ok := this.cache[k]
	//If the item does not exist in the cache
	if !ok {
		//First clone the value so we can use it in the notification.
		itemClone := this.cloner.Clone(v)
		//Place the value in the cache
		this.cache[k] = v
		//Send the notification using the clone outside the current go routine
		if this.listener != nil {
			go this.listener.ModelItemAdded(itemClone)
		}
		return nil
	}
	//Clone the existing item
	itemClone := this.cloner.Clone(item)
	//Create a new updater
	putUpdater := updater.NewUpdater(this.introspector, true)
	//update the item clone with the new element where nil is valid
	err := putUpdater.Update(itemClone, v)
	if err != nil {
		return err
	}
	//if there are changes, then nothing to do
	changes := putUpdater.Changes()
	if changes == nil {
		return nil
	}
	//Apply the changes to the existing item
	for _, change := range changes {
		change.Apply(item)
	}

	go func() {
		if this.listener != nil {
			this.listener.PropertyChangeNotification(this.ToNotificationsSet(changes))
		}
	}()
	return nil
}

func (this *Cache) Patch(k string, v interface{}) error {
	this.mtx.Lock()
	defer this.mtx.Unlock()

	item, ok := this.cache[k]
	//If the item does not exist in the cache
	if !ok {
		return errors.New("Key " + k + " not found")
	}
	//Clone the existing item
	itemClone := this.cloner.Clone(item)
	//Create a new updater
	putUpdater := updater.NewUpdater(this.introspector, false)
	//update the item clone with the new element where nil is valid
	err := putUpdater.Update(itemClone, v)
	if err != nil {
		return err
	}

	//if there are changes, then nothing to do
	changes := putUpdater.Changes()
	if changes == nil {
		return nil
	}

	//Apply the changes to the existing item
	for _, change := range changes {
		change.Apply(item)
	}
	go func() {
		if this.listener != nil {
			this.listener.PropertyChangeNotification(this.ToNotificationsSet(changes))
		}
	}()
	return nil
}

func (this *Cache) Delete(k string) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	item, ok := this.cache[k]
	if !ok {
		return
	}
	delete(this.cache, k)
	if this.listener != nil {
		go this.listener.ModelItemDeleted(item)
	}
}

func (this *Cache) Collect(f func(interface{}) interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	this.mtx.RLock()
	defer this.mtx.RUnlock()
	for k, v := range this.cache {
		result[k] = f(v)
	}
	return result
}

func (this *Cache) ToNotificationsSet(changes []*updater.Change) *types.NotificationSet {
	set := &types.NotificationSet{}
	set.Type = this.typeName
	set.Source = this.source
	set.Notifys = make([]*types.Notification, len(changes))
	this.mtx.Lock()
	set.Sequence = this.sequence
	this.sequence++
	this.mtx.Unlock()
	for i, change := range changes {
		n := &types.Notification{}
		n.PropertyId = change.PropertyId()
		obj := object.New([]byte{}, 0, "", this.introspector.Registry())
		err := obj.Add(change.OldValue())
		if err != nil {
			return nil
		}
		n.OldValue = obj.Data()

		obj = object.New([]byte{}, 0, "", this.introspector.Registry())
		err = obj.Add(change.NewValue())
		if err != nil {
			return nil
		}
		n.NewValue = obj.Data()
		set.Notifys[i] = n
	}
	return set
}
