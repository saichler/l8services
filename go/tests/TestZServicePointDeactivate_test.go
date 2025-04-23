package tests

import (
	. "github.com/saichler/l8test/go/infra/t_resources"
	"github.com/saichler/l8test/go/infra/t_servicepoints"
	"github.com/saichler/layer8/go/overlay/health"
	"testing"
)

// Until i find the deactivate bug, run this test at the end
func TestZServicePointDeactivate(t *testing.T) {
	nic := topo.VnicByVnetNum(2, 2)
	nic2 := topo.VnicByVnetNum(1, 3)
	WaitForCondition(func() bool {
		hc := health.Health(nic.Resources())
		hp := hc.HealthPoint(nic.Resources().SysConfig().LocalUuid)
		for k, _ := range hp.Services.ServiceToAreas {
			if k == "Tests" {
				return true
			}
		}
		return false
	}, 5, t, "Service Point was not found")

	nic.Resources().ServicePoints().DeActivate(t_servicepoints.ServiceName, 0, nic.Resources(), nic)
	defer func() {
		topo.ReActivateTestService(nic)
	}()

	WaitForCondition(func() bool {
		hc := health.Health(nic2.Resources())
		hp := hc.HealthPoint(nic.Resources().SysConfig().LocalUuid)
		for k, _ := range hp.Services.ServiceToAreas {
			if k == "Tests" {
				return false
			}
		}
		return true
	}, 5, t, "Service Point exist")

}
