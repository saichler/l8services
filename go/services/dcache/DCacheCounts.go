package dcache

func (this *DCache) AddCountFunc(name string, f func(interface{}) (bool, string)) {
	this.cache.AddCountFunc(name, f)
}
