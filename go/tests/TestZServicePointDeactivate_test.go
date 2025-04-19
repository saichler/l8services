package tests

import (
	. "github.com/saichler/l8test/go/infra/t_resources"
	"github.com/saichler/l8test/go/infra/t_servicepoints"
	"github.com/saichler/layer8/go/overlay/health"
	"testing"
	"time"
)

func TestServicePointDeactivate(t *testing.T) {
	nic := topo.VnicByVnetNum(2, 2)
	nic2 := topo.VnicByVnetNum(1, 3)
	hc := health.Health(nic.Resources())
	hp := hc.HealthPoint(nic.Resources().SysConfig().LocalUuid)
	foundBefore := false
	for k, _ := range hp.Services.ServiceToAreas {
		if k == "Tests" {
			foundBefore = true
		}
	}
	if !foundBefore {
		Log.Fail(t, "Did not find service")
		return
	}
	nic.Resources().ServicePoints().DeActivate(t_servicepoints.ServiceName, 0, nic.Resources(), nic)
	defer func() {
		nic.Resources().ServicePoints().Activate(t_servicepoints.ServicePointType, t_servicepoints.ServiceName, 0, nic.Resources(), nil, nic.Resources().SysConfig().LocalAlias)
	}()

	time.Sleep(time.Second)

	hc = health.Health(nic2.Resources())
	hp = hc.HealthPoint(nic.Resources().SysConfig().LocalUuid)

	foundAfter := false
	for k, _ := range hp.Services.ServiceToAreas {
		if k == "Tests" {
			foundAfter = true
		}
	}
	if foundAfter {
		Log.Fail(t, "Found service")
		return
	}

}
