package tests

import (
	"fmt"
	"github.com/saichler/reflect/go/reflect/inspect"
	"github.com/saichler/reflect/go/reflect/updater"
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/shared/go/share/registry"
	"github.com/saichler/shared/go/tests"
	"github.com/saichler/shared/go/types"
	"testing"
	"time"
)

func TestSubStructProperty(t *testing.T) {
	_introspect := inspect.NewIntrospect(registry.NewRegistry())
	node, err := _introspect.Inspect(&tests.TestProto{})
	if err != nil {
		log.Fail(t, "failed with inspect: ", err.Error())
		return
	}
	_introspect.AddDecorator(types.DecoratorType_Primary, []string{"MyString"}, node)

	aside := &tests.TestProto{MyString: "Hello"}
	zside := &tests.TestProto{MyString: "Hello"}
	yside := &tests.TestProto{MyString: "Hello"}
	zside.MySingle = &tests.TestProtoSub{MyInt64: time.Now().Unix()}

	putUpdater := updater.NewUpdater(_introspect, false)

	putUpdater.Update(aside, zside)

	changes := putUpdater.Changes()

	for _, change := range changes {
		obj := object.NewEncode([]byte{}, 0)
		err = obj.Add(change.NewValue())
		if err != nil {
			log.Fail(t, "failed with inspect: ", err.Error())
		}
		data := obj.Data()
		obj = object.NewDecode(data, 0, "", _introspect.Registry())
		newval, err := obj.Get()
		if err != nil {
			log.Fail(t, "failed with inspect: ", err.Error())
		}
		v := newval.(*tests.TestProtoSub)
		fmt.Println("newval=", v)
	}
	fmt.Println(yside)
}
