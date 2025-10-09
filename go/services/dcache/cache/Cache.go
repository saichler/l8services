package cache

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/saichler/l8types/go/ifs"
)

type Cache struct {
	cache     map[string]interface{}
	order     []string
	key2order map[string]int
	stats     map[string]int32
	statsFunc map[string]func(interface{}) bool
	stamp     int64
	queries   map[string]*DQuery
}

func NewCache() *Cache {
	return &Cache{cache: make(map[string]interface{}),
		order: make([]string, 0), key2order: make(map[string]int),
		queries: make(map[string]*DQuery)}
}

func (this *Cache) RemoveFromStats(key string) (interface{}, bool) {
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

func (this *Cache) AddToStats(value interface{}) {
	if this.statsFunc != nil {
		for stat, f := range this.statsFunc {
			if f(value) {
				this.stats[stat]++
			}
		}
	}
}

func (this *Cache) Put(key string, value interface{}) {
	_, ok := this.RemoveFromStats(key)
	this.cache[key] = value
	if !ok {
		this.order = append(this.order, key)
		this.stamp = time.Now().Unix()
		this.key2order[key] = len(this.order) - 1
	}
	this.AddToStats(value)
}

func (this *Cache) Get(key string) (interface{}, bool) {
	item, ok := this.cache[key]
	return item, ok
}

func (this *Cache) Delete(key string) (interface{}, bool) {
	item, ok := this.RemoveFromStats(key)
	delete(this.cache, key)
	this.stamp = time.Now().Unix()
	return item, ok
}

func (this *Cache) Size() int {
	return len(this.cache)
}

func (this *Cache) Cache() map[string]interface{} {
	return this.cache
}

func (this *Cache) Fetch(start, blockSize int, q ifs.IQuery) []interface{} {
	fmt.Println("Fetch invoked")
	dq, ok := this.queries[q.Hash()]
	if !ok {
		fmt.Println("Query not found, creating it")
		dq = NewDQuery(q)
		this.queries[q.Hash()] = dq
	}
	if dq.stamp != this.stamp {
		fmt.Println("Query stamp changed")
		qrt := reflect.ValueOf(q.Criteria())
		if (!qrt.IsValid() || qrt.IsNil()) && strings.TrimSpace(q.SortBy()) == "" {
			fmt.Println("Query sort by is empty and no criteria")
			dq.prepare(this.cache, this.order, this.stamp)
		} else {
			fmt.Println("Query has criteria and sortby - len of cache ", len(this.cache), " cr ", q.Criteria(), " - -", q.SortBy())
			dq.prepare(this.cache, nil, this.stamp)
		}
	}
	result := make([]interface{}, 0)
	for i := start; i < len(dq.data); i++ {
		key := dq.data[i]
		value, ok := this.cache[key]
		if ok {
			result = append(result, value)
		}
		if blockSize == 0 {
			continue
		}
		if len(result) >= blockSize {
			break
		}
	}
	return result
}

func (this *Cache) Stats() map[string]int32 {
	return this.stats
}

func (this *Cache) AddStatsFunc(name string, f func(interface{}) bool) {
	if this.statsFunc == nil {
		this.statsFunc = make(map[string]func(interface{}) bool)
		this.stats = make(map[string]int32)
	}
	this.statsFunc[name] = f
	if len(this.cache) > 0 {
		for _, elem := range this.cache {
			if f(elem) {
				this.stats[name]++
			}
		}
	}
}
