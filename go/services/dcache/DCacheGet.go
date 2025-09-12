package dcache

import (
	"errors"

	"github.com/saichler/l8utils/go/utils/strings"
)

func (this *DCache) Get(v interface{}) (interface{}, error) {
	k, err := this.keyFor(v)
	if err != nil {
		return nil, err
	}
	if k == "" {
		return nil, errors.New("Interface does not contain the Key attributes")
	}

	this.mtx.RLock()
	defer this.mtx.RUnlock()

	if this.cacheEnabled() {
		item, ok := this.cache[k]
		if ok {
			itemClone := this.cloner.Clone(item)
			return itemClone, nil
		}
	} else {
		item, err := this.store.Get(k)
		if err == nil {
			return item, nil
		}
		errors.New(strings.New("Cache:", this.serviceName, ":", this.serviceArea, " ", err.Error()).String())
	}
	return nil, errors.New("Not found in the cache")
}
