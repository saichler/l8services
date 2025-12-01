package base

import (
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8api"
)

func (this *BaseService) Collect(f func(interface{}) (bool, interface{})) map[string]interface{} {
	if this.cache == nil {
		return nil
	}
	return this.cache.Collect(f)
}

func (this *BaseService) All() map[string]interface{} {
	if this.cache == nil {
		return nil
	}
	return this.cache.Collect(all)
}

func (this *BaseService) ServiceName() string {
	if this.cache == nil {
		return ""
	}
	return this.cache.ServiceName()
}

func (this *BaseService) ServiceArea() byte {
	if this.cache == nil {
		return 0
	}
	return this.cache.ServiceArea()
}

func (this *BaseService) Size() int {
	if this.cache == nil {
		return 0
	}
	return this.cache.Size()
}

func (this *BaseService) Fetch(start, blockSize int, q ifs.IQuery) ([]interface{}, *l8api.L8MetaData) {
	if this.cache == nil {
		return nil, nil
	}
	return this.cache.Fetch(start, blockSize, q)
}

func (this *BaseService) AddMetadataFunc(name string, f func(interface{}) (bool, string)) {
	if this.cache == nil {
		return
	}
	this.cache.AddMetadataFunc(name, f)
}

func all(i interface{}) (bool, interface{}) {
	return true, i
}
