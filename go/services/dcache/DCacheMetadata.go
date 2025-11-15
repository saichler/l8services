package dcache

func (this *DCache) AddMetadataFunc(name string, f func(interface{}) (bool, string)) {
	this.cache.AddMetadataFunc(name, f)
}
