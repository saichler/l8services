package dcache

func (this *DCache) Sync() {
	if this.listener == nil {
		return
	}
	allItems := this.GetAll()
	for key, item := range allItems {
		n, _ := this.createSyncNotification(item, key)
		this.listener.PropertyChangeNotification(n)
	}
}

func (this *DCache) GetAll() map[string]interface{} {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	if this.cacheEnabled() {
		result := make(map[string]interface{})
		for key, item := range this.cache.cache {
			itemClone := this.cloner.Clone(item)
			result[key] = itemClone
		}
		return result
	}
	return this.store.Collect(all)
}

func all(i interface{}) (bool, interface{}) {
	return true, i
}
