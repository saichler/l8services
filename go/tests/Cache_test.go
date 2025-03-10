package tests

import (
	"github.com/saichler/reflect/go/reflect/inspect"
	"github.com/saichler/reflect/go/tests/utils"
	"github.com/saichler/servicepoints/go/points/cache"
	"github.com/saichler/types/go/testtypes"
	"testing"
	"time"
)

func TestCacheListener(t *testing.T) {
	ni := inspect.NewIntrospect(globals.Registry())
	c := cache.NewModelCache("", nil, ni)
	item1 := utils.CreateTestModelInstance(1)
	ni.Inspect(item1)
	err := c.Put(item1.MyString, item1)
	if err != nil {
		log.Fail(t, err.Error())
		return
	}
	item2 := utils.CreateTestModelInstance(1)
	item2.MyEnum = testtypes.TestEnum_ValueTwo
	err = c.Update(item2.MyString, item2)
	if err != nil {
		log.Fail(t, err.Error())
		return
	}
	time.Sleep(time.Second)
}
