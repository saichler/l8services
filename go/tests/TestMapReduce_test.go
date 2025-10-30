package tests

import (
	"testing"

	"github.com/saichler/l8test/go/infra/t_service"
	"github.com/saichler/l8types/go/ifs"
)

func TestMapReduce(t *testing.T) {
	nic := topo.VnicByVnetNum(1, 1)
	resp := nic.Request("", t_service.ServiceName, 0, ifs.MapR_GET, nil, 30)
	if resp.Error() != nil {
		nic.Resources().Logger().Fail(t, resp.Error().Error())
		return
	}
	if len(resp.Elements()) != 9 {
		nic.Resources().Logger().Fail(t, "Expected 9")
		return
	}
}
