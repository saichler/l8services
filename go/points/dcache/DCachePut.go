package dcache

import (
	"github.com/saichler/reflect/go/reflect/updating"
	"github.com/saichler/types/go/types"
)

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
	putUpdater := updating.NewUpdater(this.resources.Introspector(), true)

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
