package base

import (
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8api"
)

func (this *BaseService) Collect(f func(interface{}) (bool, interface{})) map[string]interface{} {
	return this.cache.Collect(f)
}

func (this *BaseService) All() map[string]interface{} {
	return this.cache.Collect(all)
}

func (this *BaseService) ServiceName() string {
	return this.cache.ServiceName()
}

func (this *BaseService) ServiceArea() byte {
	return this.cache.ServiceArea()
}

func (this *BaseService) Size() int {
	return this.cache.Size()
}

func (this *BaseService) Fetch(start, blockSize int, q ifs.IQuery) ([]interface{}, *l8api.L8Counts) {
	return this.cache.Fetch(start, blockSize, q)
}

func (this *BaseService) AddCountFunc(name string, f func(interface{}) (bool, string)) {
	this.cache.AddCountFunc(name, f)
}

func all(i interface{}) (bool, interface{}) {
	return true, i
}
