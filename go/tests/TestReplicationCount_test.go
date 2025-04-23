package tests

import (
	"fmt"
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_servicepoints"
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/servicepoints/go/points/replication"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/testtypes"
	"strconv"
	"testing"
	"time"
)

func TestTransactionReplication(t *testing.T) {
	defer reset("TestTransactionReplication")
	nic := topo.VnicByVnetNum(1, 1)

	time.Sleep(time.Second * 5)
	index, _ := replication.ReplicationIndex("Tests", 2, nic.Resources())
	if len(index.EndPoints) != 9 {
		Log.Fail(t, "Expected 9 end points, got ", len(index.EndPoints))
		return
	}

	hc := health.Health(nic.Resources())
	hp := hc.HealthPoint(nic.Resources().SysConfig().LocalUuid)
	fmt.Println(hp)

	if !doRound(2, 0, t) {
		return
	}

	time.Sleep(time.Second)

	if !doRound(4, 1, t) {
		return
	}
}

func doRound(ecount, score int, t *testing.T) bool {
	pb := &testtypes.TestProto{MyString: "test" + strconv.Itoa(score)}
	eg := topo.VnicByVnetNum(2, 1)
	resp := eg.SingleRequest(ServiceName, 2, common.POST, pb)
	if resp.Error() != nil {
		Log.Fail(t, resp.Error().Error())
		return false
	}

	count := 0
	handlers := topo.AllRepHandlers()
	for _, ts := range handlers {
		count += ts.PostN()
	}
	if count != ecount {
		Log.Fail(t, "Expected count to be ", ecount, "got ", count)
		return false
	}
	return true
	/*
		rep := hp.ReplicasFor("TestProto", 0, 2)
		for _, r := range rep {
			if int(r) != score {
				Log.Fail(t, "Expected score to be ", score, " got ", r)
				return false
			}
		}
	return true*/
}
