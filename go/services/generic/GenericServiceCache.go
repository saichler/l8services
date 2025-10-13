package generic

import "github.com/saichler/l8types/go/ifs"

func (this *GenericService) Collect(f func(interface{}) (bool, interface{})) map[string]interface{} {
	return this.cache.Collect(f)
}

func (this *GenericService) ServiceName() string {
	return this.cache.ServiceName()
}

func (this *GenericService) ServiceArea() byte {
	return this.cache.ServiceArea()
}

func (this *GenericService) Size() int {
	return this.cache.Size()
}

func (this *GenericService) Fetch(start, blockSize int, q ifs.IQuery) []interface{} {
	return this.cache.Fetch(start, blockSize, q)
}

func (this *GenericService) AddStatFunc(name string, f func(interface{}) bool) {
	this.cache.AddStatFunc(name, f)
}

func (this *GenericService) Stats() map[string]int32 {
	return this.cache.Stats()
}

func (this *GenericService) Sync() {
	allItems := this.cache.Collect(all)
	for key, item := range allItems {
		n, _ := this.cache.CreateSyncNotification(item, key)
		this.vnic.PropertyChangeNotification(n)
	}
}

func all(i interface{}) (bool, interface{}) {
	return true, i
}
