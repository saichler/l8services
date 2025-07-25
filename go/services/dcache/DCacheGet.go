package dcache

func (this *DCache) Get(k string) interface{} {
	this.mtx.RLock()
	defer this.mtx.RUnlock()

	if this.cacheEnabled() {
		item, ok := this.cache[k]
		if ok {
			itemClone := this.cloner.Clone(item)
			return itemClone
		}
	} else {
		item, err := this.store.Get(k)
		if err == nil {
			return item
		}
		this.resources.Logger().Error("Cache:", this.serviceName, ":", this.serviceArea, " ", err.Error())
	}
	return nil
}
