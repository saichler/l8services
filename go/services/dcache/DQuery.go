package dcache

import (
	"encoding/binary"
	"fmt"
	"net"
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
		fmt.Println("Order is not nil")
		data = order
	} else {
		fmt.Println("Order is nil")
		for k, v := range cache {
			if this.query.Match(v) {
				data = append(data, k)
			}
		}
	}
	fmt.Println("Query has ", len(data), " sorting")
	sort.Slice(data, func(i, j int) bool {
		if order == nil && this.query.SortBy() != "" {
			v1 := this.query.SortByValue(cache[data[i]])
			v2 := this.query.SortByValue(cache[data[j]])
			if v1 != nil && v2 != nil {
				return lessThan(v1, v2)
			}
		}
		return lessThan(data[i], data[j])
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
			// Check if both strings are IPv4 addresses
			ip1 := net.ParseIP(v1)
			ip2 := net.ParseIP(v2)
			if ip1 != nil && ip2 != nil {
				// Check if they are IPv4 (not IPv6)
				ip1v4 := ip1.To4()
				ip2v4 := ip2.To4()
				if ip1v4 != nil && ip2v4 != nil {
					// Convert IPv4 to uint32 for comparison
					num1 := binary.BigEndian.Uint32(ip1v4)
					num2 := binary.BigEndian.Uint32(ip2v4)
					return num1 < num2
				}
			}
			// If not both IPv4, compare as regular strings
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
