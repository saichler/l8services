package dcache

import "github.com/saichler/l8types/go/ifs"

type ReplicationCache struct {
	cache    map[int]ifs.IDistributedCache
	resource ifs.IResources
	store    ifs.IStorage
}

func NewReplicationCache(r ifs.IResources, store ifs.IStorage) ifs.IReplicationCache {
	c := &ReplicationCache{}
	c.cache = make(map[int]ifs.IDistributedCache)
	c.resource = r
	c.store = store
	return c
}

func (this *ReplicationCache) Post(elem interface{}, rep int) error {
	repCache, ok := this.cache[rep]
	if !ok {
		repCache = NewDistributedCacheWithStorage("", 0, elem,
			nil, nil, this.resource, this.store)
		this.cache[rep] = repCache
	}
	_, err := repCache.Post(elem, true)
	return err
}

func (this *ReplicationCache) Put(elem interface{}, rep int) error {
	repCache, ok := this.cache[rep]
	if !ok {
		repCache = NewDistributedCacheWithStorage("", 0, elem,
			nil, nil, this.resource, this.store)
		this.cache[rep] = repCache
	}
	_, err := repCache.Put(elem, true)
	return err
}

func (this *ReplicationCache) Patch(elem interface{}, rep int) error {
	repCache, ok := this.cache[rep]
	if !ok {
		repCache = NewDistributedCacheWithStorage("", 0, elem,
			nil, nil, this.resource, this.store)
		this.cache[rep] = repCache
	}
	_, err := repCache.Patch(elem, true)
	return err
}

func (this *ReplicationCache) Delete(elem interface{}, rep int) error {
	repCache, ok := this.cache[rep]
	if !ok {
		repCache = NewDistributedCacheWithStorage("", 0, elem,
			nil, nil, this.resource, this.store)
		this.cache[rep] = repCache
	}
	_, err := repCache.Delete(elem, true)
	return err
}

func (this *ReplicationCache) Get(elem interface{}, rep int) (interface{}, error) {
	repCache, ok := this.cache[rep]
	if !ok {
		repCache = NewDistributedCacheWithStorage("", 0, elem,
			nil, nil, this.resource, this.store)
		this.cache[rep] = repCache
	}
	return repCache.Get(elem)
}
