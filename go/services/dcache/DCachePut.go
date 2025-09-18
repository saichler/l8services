package dcache

import (
	"github.com/saichler/l8types/go/types/l8notify"
)

func (this *DCache) Put(v interface{}, sourceNotification ...bool) (*l8notify.L8NotificationSet, error) {
	//Seems that the post is handling also a put situation, where the item is replaced
	return this.Post(v, sourceNotification...)
}
