package dcache

func (this *DCache) Sync() {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	if this.listener == nil {
		return
	}
	for key, item := range this.cache {
		n, _ := this.createSyncNotification(item, key)
		go this.listener.PropertyChangeNotification(n)
	}
}
