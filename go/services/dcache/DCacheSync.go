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
	if this.cacheEnabled() {
		result := make(map[string]interface{})
		this.cache.Range(func(k, v interface{}) bool {
			itemClone := this.cloner.Clone(v)
			result[k.(string)] = itemClone
			return true
		})
		return result
	}
	return this.store.Collect(all)
}

func all(i interface{}) (bool, interface{}) {
	return true, i
}
