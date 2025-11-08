package dcache

import (
	"github.com/saichler/l8types/go/types/l8notify"
)

func (this *DCache) Patch(v interface{}, sourceNotification ...bool) (*l8notify.L8NotificationSet, error) {
	createNotification := !(sourceNotification != nil && len(sourceNotification) > 0 && sourceNotification[0])
	n, e := this.cache.Patch(v, createNotification)
	if this.listener != nil && createNotification && e == nil && n != nil {
		this.nQueue.Add(n)
	}
	return n, e
}
