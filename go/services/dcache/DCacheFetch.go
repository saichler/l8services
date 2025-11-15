package dcache

import (
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8api"
)

func (this *DCache) Fetch(start, blockSize int, q ifs.IQuery) ([]interface{}, *l8api.L8MetaData) {
	return this.cache.Fetch(start, blockSize, q)
}
