package tests

import (
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_servicepoints"
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/types/go/testtypes"
	"github.com/saichler/types/go/types"
	"testing"
	"time"
)

func TestTransactionReplication(t *testing.T) {
	defer reset("TestTransactionReplication")
	setTransactionMode(2)

	if !doRound(2, 0, t) {
		return
	}

	time.Sleep(time.Second)

	if !doRound(4, 1, t) {
		return
	}
}

func doRound(ecount, score int, t *testing.T) bool {
	pb := &testtypes.TestProto{MyString: "test"}
	eg := topo.VnicByVnetNum(2, 1)
	resp := eg.SingleRequest(ServiceName, 0, types.Action_POST, pb)
	if resp.Error() != nil {
		Log.Fail(t, resp.Error().Error())
		return false
	}

	count := 0
	handlers := topo.AllHandlers()
	for _, ts := range handlers {
		count += ts.PostN()
	}
	if count != ecount {
		Log.Fail(t, "Expected count to be ", ecount, "got ", count)
		return false
	}

	hp := health.Health(eg.Resources())
	rep := hp.ReplicasFor("TestProto", 0, 2)
	for _, r := range rep {
		if int(r) != score {
			Log.Fail(t, "Expected score to be ", score, " got ", r)
			return false
		}
	}
	return true
}
