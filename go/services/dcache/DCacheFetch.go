package dcache

func (this *DCache) Fetch(start, blockSize int) []interface{} {
	this.mtx.RLock()
	defer this.mtx.RUnlock()
	values := this.cache.fetch(start, blockSize)
	result := make([]interface{}, len(values))
	for i, v := range values {
		result[i] = this.cloner.Clone(v)
	}
	return result
}
