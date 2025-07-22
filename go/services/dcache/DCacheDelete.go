package dcache

import (
	"errors"
	"github.com/saichler/l8types/go/types"
)

func (this *DCache) Delete(k string, sourceNotification ...bool) (*types.NotificationSet, error) {
	this.mtx.Lock()
	defer this.mtx.Unlock()

	var n *types.NotificationSet
	var e error
	var item interface{}
	var ok bool
	isNotification := (sourceNotification != nil && len(sourceNotification) > 0 && sourceNotification[0])

	if this.cacheEnabled() {
		item, ok = this.cache[k]
		if !ok {
			return nil, errors.New("Key " + k + " not found")
		}
		delete(this.cache, k)
	}

	if this.store != nil {
		item, e = this.store.Delete(k)
		if e != nil {
			return nil, e
		}
	}

	if this.listener != nil && !isNotification {
		n, e = this.createDeleteNotification(item, k)
		if e != nil {
			return n, e
		}
		go this.listener.PropertyChangeNotification(n)
	}
	return n, nil
}
