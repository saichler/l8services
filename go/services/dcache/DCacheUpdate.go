package dcache

import (
	"github.com/saichler/l8types/go/types"
	"github.com/saichler/reflect/go/reflect/updating"
)

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
