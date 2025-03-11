package tests

import (
	"fmt"
	"github.com/saichler/reflect/go/reflect/introspecting"
	"github.com/saichler/reflect/go/reflect/updating"
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/shared/go/share/registry"
	"github.com/saichler/types/go/testtypes"
	"github.com/saichler/types/go/types"
	"testing"
	"time"
)

func TestSubStructProperty(t *testing.T) {
	_introspect := introspecting.NewIntrospect(registry.NewRegistry())
	node, err := _introspect.Inspect(&testtypes.TestProto{})
	if err != nil {
		log.Fail(t, "failed with inspect: ", err.Error())
		return
	}
	_introspect.AddDecorator(types.DecoratorType_Primary, []string{"MyString"}, node)

	aside := &testtypes.TestProto{MyString: "Hello"}
	zside := &testtypes.TestProto{MyString: "Hello"}
	yside := &testtypes.TestProto{MyString: "Hello"}
	zside.MySingle = &testtypes.TestProtoSub{MyInt64: time.Now().Unix()}

	putUpdater := updating.NewUpdater(_introspect, false)

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
		v := newval.(*testtypes.TestProtoSub)
		fmt.Println("newval=", v)
	}
	fmt.Println(yside)
}
