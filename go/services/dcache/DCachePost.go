package dcache

import (
	"errors"

	"github.com/saichler/l8reflect/go/reflect/updating"
	"github.com/saichler/l8types/go/types/l8notify"
)

func (this *DCache) Post(v interface{}, sourceNotification ...bool) (*l8notify.L8NotificationSet, error) {
	k, err := this.PrimaryKeyFor(v)
	if err != nil {
		return nil, err
	}
	if k == "" {
		return nil, errors.New("Interface does not contain the Key attributes")
	}
	//Make sure we clone the input value, so the caller don't have a reference to the cache element
	v = this.cloner.Clone(v)

	this.mtx.Lock()
	defer this.mtx.Unlock()

	var n *l8notify.L8NotificationSet
	var e error
	var item interface{}
	var ok bool

	isNotification := (sourceNotification != nil && len(sourceNotification) > 0 && sourceNotification[0])

	if this.cacheEnabled() {
		item, ok = this.cache.Get(k)
	} else {
		item, e = this.store.Get(k)
		ok = e == nil
	}

	//If the item does not exist in the cache
	if !ok {
		//First clone the value so we can use it in the notification.
		itemClone := this.cloner.Clone(v)
		if this.cacheEnabled() {
			//Place the value in the cache
			this.cache.Put(k, v)
		}
		if this.store != nil {
			e = this.store.Put(k, v)
			if e != nil {
				this.resources.Logger().Error(e.Error())
			}
		}
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

	//Clone the instance so it won't be able to be updated outside the scope
	vClone := this.cloner.Clone(v)

	if this.cacheEnabled() {
		//Place the value in the cache
		this.cache.Put(k, vClone)
	}

	if this.store != nil {
		e = this.store.Put(k, vClone)
		if e != nil {
			this.resources.Logger().Error(e.Error())
		}
	}

	//if the source is a notification, don't send notification
	if isNotification {
		return n, e
	}

	//From this point onward, the item is no longer in the cache
	//so we don't need to clone it
	//itemClone := this.cloner.Clone(item)

	//Create a new updater
	putUpdater := updating.NewUpdater(this.resources, true, true)

	//update the item clone with the new element where nil is valid
	e = putUpdater.Update(item, vClone)
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
