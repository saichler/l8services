package tests

import (
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_servicepoints"
	"github.com/saichler/reflect/go/reflect/introspecting"
	"github.com/saichler/reflect/go/tests/utils"
	"github.com/saichler/servicepoints/go/points/cache"
	"github.com/saichler/types/go/testtypes"
	"testing"
)

func TestCacheListener(t *testing.T) {
	ni := introspecting.NewIntrospect(globals.Registry())
	c := cache.NewModelCache(ServiceName, 0, "TestProto", "", nil, ni)
	item1 := utils.CreateTestModelInstance(1)
	ni.Inspect(item1)
	_, err := c.Put(item1.MyString, item1)
	if err != nil {
		Log.Fail(t, err.Error())
		return
	}
	item2 := utils.CreateTestModelInstance(1)
	item2.MyEnum = testtypes.TestEnum_ValueTwo
	_, err = c.Update(item2.MyString, item2)
	if err != nil {
		Log.Fail(t, err.Error())
		return
	}
}
