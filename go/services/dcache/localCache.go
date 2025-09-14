package dcache

type localCache struct {
	cache     map[string]interface{}
	order     []string
	key2order map[string]int
	stats     map[string]int32
	statsFunc map[string]func(interface{}) bool
}

func newLocalCache() *localCache {
	return &localCache{cache: make(map[string]interface{}), order: make([]string, 0), key2order: make(map[string]int)}
}

func (this *localCache) removeFromStats(key string) (interface{}, bool) {
	old, ok := this.cache[key]
	if ok && this.statsFunc != nil {
		for stat, f := range this.statsFunc {
			if f(old) {
				this.stats[stat]--
			}
		}
	}
	return old, ok
}

func (this *localCache) addToStats(value interface{}) {
	if this.statsFunc != nil {
		for stat, f := range this.statsFunc {
			if f(value) {
				this.stats[stat]++
			}
		}
	}
}

func (this *localCache) put(key string, value interface{}) {
	_, ok := this.removeFromStats(key)
	this.cache[key] = value
	if !ok {
		this.order = append(this.order, key)
		this.key2order[key] = len(this.order) - 1
	}
	this.addToStats(value)
}

func (this *localCache) get(key string) (interface{}, bool) {
	item, ok := this.cache[key]
	return item, ok
}

func (this *localCache) delete(key string) (interface{}, bool) {
	item, ok := this.removeFromStats(key)
	delete(this.cache, key)
	return item, ok
}

func (this *localCache) size() int {
	return len(this.cache)
}

func (this *localCache) fetch(start, blockSize int) []interface{} {
	result := make([]interface{}, 0)
	for i := start; i < len(this.order); i++ {
		key := this.order[i]
		value, ok := this.cache[key]
		if ok {
			result = append(result, value)
		}
		if len(result) >= blockSize {
			break
		}
	}
	return result
}

func (this *localCache) addStatsFunc(name string, f func(interface{}) bool) {
	if this.statsFunc == nil {
		this.statsFunc = make(map[string]func(interface{}) bool)
		this.stats = make(map[string]int32)
	}
	this.statsFunc[name] = f
}
