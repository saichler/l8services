package dcache

func (this *DCache) Get(v interface{}) (interface{}, error) {
	return this.cache.Get(v)
}
