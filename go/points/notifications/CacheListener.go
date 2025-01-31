package notifications

import (
	"github.com/saichler/reflect/go/reflect/inspect"
	"github.com/saichler/shared/go/share/registry"
	"github.com/saichler/shared/go/tests"
	"github.com/saichler/shared/go/types"
)

type CacheListener struct {
}

func (this *CacheListener) ModelItemAdded(interface{}) {

}

func (this *CacheListener) ModelItemDeleted(interface{}) {

}

var r = registry.NewRegistry()
var i = inspect.NewIntrospect(r)

func init() {
	i.Inspect(&tests.TestProto{})
}
func (this *CacheListener) PropertyChangeNotification(notify *types.NotificationSet) {
	/*
		for _, n := range notify {
			p, e := property.PropertyOf(n.propertyID, i)
			if e != nil {
				panic(e)
			}
			obj := object.New(n.newValue, 0, "", r)
			v, e := obj.Get()
			if e != nil {
				panic(e)
			}
			_, root, e := p.Set(nil, v)
			if e != nil {
				panic(e)
			}
			fmt.Println("root:", reflect.ValueOf(root).Elem().Type().Name())
		}*/
}
