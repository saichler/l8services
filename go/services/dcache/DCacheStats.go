package dcache

func (this *DCache) AddStatFunc(name string, f func(interface{}) bool) {
	this.cache.AddStatFunc(name, f)
}

func (this *DCache) Stats() map[string]int32 {
	return this.cache.Stats()
}
