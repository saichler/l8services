package dcache

import (
	"errors"

	"github.com/saichler/l8types/go/types/l8notify"
)

func (this *DCache) Delete(v interface{}, sourceNotification ...bool) (*l8notify.L8NotificationSet, error) {
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
		item, ok = this.cache.Delete(k)
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
