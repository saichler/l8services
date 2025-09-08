package dcache

import (
	"github.com/saichler/l8types/go/types"
)

func (this *DCache) Put(k string, v interface{}, sourceNotification ...bool) (*types.NotificationSet, error) {
	//Seems that the post is handling also a put situation, where the item is replaced
	return this.Post(k, v, sourceNotification...)
}
