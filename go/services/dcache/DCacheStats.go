package dcache

func (this *DCache) AddStatFunc(name string, f func(interface{}) bool) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	this.cache.addStatsFunc(name, f)
}

func (this *DCache) Stats() map[string]int32 {
	this.mtx.RLock()
	defer this.mtx.RUnlock()
	result := make(map[string]int32)
	for k, v := range this.cache.stats {
		result[k] = v
	}
	return result
}
