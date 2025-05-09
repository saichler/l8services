package tests

import (
	. "github.com/saichler/l8test/go/infra/t_resources"
	"github.com/saichler/reflect/go/reflect/introspecting"
	"github.com/saichler/reflect/go/reflect/updating"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8utils/go/utils/registry"
	"github.com/saichler/l8types/go/testtypes"
	"testing"
	"time"
)

func TestSubStructProperty(t *testing.T) {
	_introspect := introspecting.NewIntrospect(registry.NewRegistry())
	node, err := _introspect.Inspect(&testtypes.TestProto{})
	if err != nil {
		Log.Fail(t, "failed with inspect: ", err.Error())
		return
	}
	introspecting.AddPrimaryKeyDecorator(node, "MyString")

	aside := &testtypes.TestProto{MyString: "Hello"}
	zside := &testtypes.TestProto{MyString: "Hello"}
	yside := &testtypes.TestProto{MyString: "Hello"}
	zside.MySingle = &testtypes.TestProtoSub{MyInt64: time.Now().Unix()}

	putUpdater := updating.NewUpdater(_introspect, false, false)

	putUpdater.Update(aside, zside)

	changes := putUpdater.Changes()

	for _, change := range changes {
		obj := object.NewEncode()
		err = obj.Add(change.NewValue())
		if err != nil {
			Log.Fail(t, "failed with inspect: ", err.Error())
		}
		data := obj.Data()
		obj = object.NewDecode(data, 0, _introspect.Registry())
		newval, err := obj.Get()
		if err != nil {
			Log.Fail(t, "failed with inspect: ", err.Error())
		}
		v := newval.(*testtypes.TestProtoSub)
		if v.MyInt64 != zside.MySingle.MyInt64 {
			Log.Fail(t, "failed with inspect: myString != zside.MyString")
			return
		}
		if yside.MyString != zside.MyString {
			Log.Fail(t, "failed with inspect: myString != zside.MyString")
			return
		}
	}
}
