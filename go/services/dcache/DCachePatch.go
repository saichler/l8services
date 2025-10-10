package dcache

import (
	"errors"

	"github.com/saichler/l8reflect/go/reflect/updating"
	"github.com/saichler/l8types/go/types/l8notify"
)

func (this *DCache) Patch(v interface{}, sourceNotification ...bool) (*l8notify.L8NotificationSet, error) {
	k, err := this.PrimaryKeyFor(v)
	if err != nil {
		return nil, err
	}
	if k == "" {
		return nil, errors.New("Interface does not contain the Key attributes")
	}

	this.mtx.Lock()
	defer this.mtx.Unlock()
	var n *l8notify.L8NotificationSet
	var e error
	var item interface{}
	var ok bool

	isNotification := (sourceNotification != nil && len(sourceNotification) > 0 && sourceNotification[0])

	if this.cacheEnabled() {
		item, ok = this.cache.get(k)
	} else {
		item, e = this.store.Get(k)
		ok = e == nil
	}

	//If the item does not exist in the cache
	if !ok {
		//We need to clone the item twice, once for the cache and another
		//for the notification
		itemClone := this.cloner.Clone(v)
		vClone := this.cloner.Clone(v)

		if this.cacheEnabled() {
			//Place the new Item clone in the cache
			this.cache.put(k, vClone)
		}

		if this.store != nil {
			//place the new item clone in the store
			e = this.store.Put(k, vClone)
		}

		//Send the notification using the clone outside the current go routine
		if this.listener != nil {
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

	patchUpdater := updating.NewUpdater(this.resources, false, false)
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

	this.cache.removeFromStats(k)

	//Apply the changes to the existing item in the cache
	for _, change := range changes {
		change.Apply(item)
	}

	this.cache.addToStats(item)

	if this.store != nil {
		e = this.store.Put(k, item)
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
