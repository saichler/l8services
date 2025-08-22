package dcache

func (this *DCache) Collect(f func(interface{}) (bool, interface{})) map[string]interface{} {
	result := map[string]interface{}{}
	if this.cacheEnabled() {
		this.cache.Range(func(k, v interface{}) bool {
			vClone := this.cloner.Clone(v)
			ok, elem := f(vClone)
			if ok {
				result[k.(string)] = elem
			}
			return true
		})
		return result
	}

	return this.store.Collect(f)
}
