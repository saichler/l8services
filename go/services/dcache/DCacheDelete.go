package dcache

import (
	"errors"

)

func (this *DCache) Delete(v interface{}, sourceNotification ...bool) (*types.NotificationSet, error) {
	k, err := this.PrimaryKeyFor(v)
	if err != nil {
		return nil, err
	}
	if k == "" {
		return nil, errors.New("Interface does not contain the Key attributes")
	}

	this.mtx.Lock()
	defer this.mtx.Unlock()

	var n *types.NotificationSet
	var e error
	var item interface{}
	var ok bool
	isNotification := (sourceNotification != nil && len(sourceNotification) > 0 && sourceNotification[0])

	if this.cacheEnabled() {
		item, ok = this.cache.delete(k)
		if !ok {
			return nil, errors.New("Key " + k + " not found")
		}
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
