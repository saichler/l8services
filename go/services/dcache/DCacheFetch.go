package dcache

import "github.com/saichler/l8types/go/ifs"

func (this *DCache) Fetch(start, blockSize int, q ifs.IQuery) []interface{} {
	return this.cache.Fetch(start, blockSize, q)
}
