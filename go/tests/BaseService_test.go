package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/saichler/l8reflect/go/tests/utils"
	"github.com/saichler/l8services/go/services/base"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/testtypes"
)

func TestBaseService(t *testing.T) {
	sc := &ifs.ServiceConfig{}
	sc.ServiceName = "base"
	sc.ServiceItem = &testtypes.TestProto{}
	sc.ServiceItemList = &testtypes.TestProtoList{}
	sc.PrimaryKey = []string{"MyString"}
	sc.Voter = true

	for vnet := 1; vnet <= 3; vnet++ {
		for vnic := 1; vnic <= 3; vnic++ {
			nic := topo.VnicByVnetNum(vnet, vnic)
			base.Activate(sc, nic)
		}
	}

	time.Sleep(time.Second)
	nic := topo.VnicByVnetNum(1, 1)
	for i := 0; i < 10; i++ {
		elem := utils.CreateTestModelInstance(i)
		h, _ := nic.Resources().Services().ServiceHandler(sc.ServiceName, 0)
		h.Post(object.New(nil, elem), nic)
	}

	time.Sleep(time.Second)

	for vnet := 1; vnet <= 3; vnet++ {
		for vnic := 1; vnic <= 3; vnic++ {
			nic = topo.VnicByVnetNum(vnet, vnic)
			h, _ := nic.Resources().Services().ServiceHandler(sc.ServiceName, 0)
			hb := h.(*base.BaseService)
			if hb.Size() != 10 {
				fmt.Println(nic.Resources().SysConfig().LocalAlias, " does not have 10 items ", hb.Size())
				t.Fail()
				return
			}
		}
	}

	time.Sleep(time.Second * 10)

	topo.RenewVnic(2, 3)

	vnic := topo.VnicByVnetNum(2, 3)
	base.Activate(sc, vnic)

	time.Sleep(time.Second * 10)

	nic = topo.VnicByVnetNum(2, 3)
	h, _ := nic.Resources().Services().ServiceHandler(sc.ServiceName, 0)
	hb := h.(*base.BaseService)
	if hb.Size() != 10 {
		fmt.Println("recover ", nic.Resources().SysConfig().LocalAlias, " does not have 10 items ", hb.Size())
		t.Fail()
		return
	}
}
