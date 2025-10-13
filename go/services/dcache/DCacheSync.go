package dcache

func (this *DCache) Sync() {
	if this.listener == nil {
		return
	}
	allItems := this.GetAll()
	for key, item := range allItems {
		n, _ := this.cache.CreateSyncNotification(item, key)
		this.listener.PropertyChangeNotification(n)
	}
}

func (this *DCache) GetAll() map[string]interface{} {
	return this.cache.Collect(all)
}

func all(i interface{}) (bool, interface{}) {
	return true, i
}
