package base

import "github.com/saichler/l8types/go/ifs"

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

func (this *BaseService) Fetch(start, blockSize int, q ifs.IQuery) []interface{} {
	return this.cache.Fetch(start, blockSize, q)
}

func (this *BaseService) AddStatFunc(name string, f func(interface{}) bool) {
	this.cache.AddStatFunc(name, f)
}

func (this *BaseService) Stats() map[string]int32 {
	return this.cache.Stats()
}

func (this *BaseService) Sync() {
	allItems := this.cache.Collect(all)
	for key, item := range allItems {
		n, _ := this.cache.CreateSyncNotification(item, key)
		this.vnic.PropertyChangeNotification(n)
	}
}

func all(i interface{}) (bool, interface{}) {
	return true, i
}
