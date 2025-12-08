package tests

import (
	"testing"

	"github.com/saichler/l8reflect/go/tests/utils"
	"github.com/saichler/l8services/go/services/dcache"
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_service"
	"github.com/saichler/l8types/go/testtypes"
)

func TestCacheListener(t *testing.T) {
	item1 := utils.CreateTestModelInstance(1)
	AddPrimaryKey(globals)

	c := dcache.NewDistributedCache(ServiceName, 0, &testtypes.TestProto{}, nil, nil, globals)

	_, err := c.Put(item1)
	if err != nil {
		Log.Fail(t, err.Error())
		return
	}
	item2 := utils.CreateTestModelInstance(1)
	item2.MyEnum = testtypes.TestEnum_ValueTwo
	_, err = c.Patch(item2)
	if err != nil {
		Log.Fail(t, err.Error())
		return
	}
}
