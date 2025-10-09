package dcache

import "github.com/saichler/l8types/go/ifs"

func (this *DCache) Fetch(start, blockSize int, q ifs.IQuery) []interface{} {
	this.mtx.RLock()
	defer this.mtx.RUnlock()
	values := this.cache.Fetch(start, blockSize, q)
	result := make([]interface{}, len(values))
	for i, v := range values {
		result[i] = this.cloner.Clone(v)
	}
	return result
}
