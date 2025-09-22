package dcache

import (
	"fmt"
	"sort"

	"github.com/saichler/l8types/go/ifs"
)

type DQuery struct {
	query ifs.IQuery
	data  []string
	stamp int64
	hash  string
}

func NewDQuery(query ifs.IQuery) *DQuery {
	dq := &DQuery{query: query}
	dq.hash = query.Hash()
	return dq
}

func (this *DQuery) Hash() string {
	return this.hash
}

func (this *DQuery) prepare(cache map[string]interface{}, order []string) {
	data := make([]string, 0)

	if order != nil {
		fmt.Println("Order is nil")
		data = order
	} else {
		fmt.Println("Order is not nil")
		for k, v := range cache {
			if this.query.Match(v) {
				data = append(data, k)
			}
		}
	}
	fmt.Println("Query has ", len(data), " sorting")
	sort.Slice(data, func(i, j int) bool {
		if order == nil {
			v1 := this.query.SortByValue(cache[data[i]])
			v2 := this.query.SortByValue(cache[data[j]])
			if v1 != nil && v2 != nil {
				return lessThan(v1, v2)
			}
		}
		return data[i] < data[j]
	})
	this.data = data
}

func lessThan(a interface{}, b interface{}) bool {
	switch v1 := a.(type) {
	case int:
		if v2, ok := b.(int); ok {
			return v1 < v2
		}
	case int64:
		if v2, ok := b.(int64); ok {
			return v1 < v2
		}
	case int32:
		if v2, ok := b.(int32); ok {
			return v1 < v2
		}
	case float64:
		if v2, ok := b.(float64); ok {
			return v1 < v2
		}
	case float32:
		if v2, ok := b.(float32); ok {
			return v1 < v2
		}
	case string:
		if v2, ok := b.(string); ok {
			return v1 < v2
		}
	case uint:
		if v2, ok := b.(uint); ok {
			return v1 < v2
		}
	case uint64:
		if v2, ok := b.(uint64); ok {
			return v1 < v2
		}
	case uint32:
		if v2, ok := b.(uint32); ok {
			return v1 < v2
		}
	}
	return false
}
