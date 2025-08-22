package tests

import (
	"strconv"
	"testing"
	"time"

	"github.com/saichler/l8services/go/services/replication"
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_service"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/testtypes"
)

func TestTransactionReplication(t *testing.T) {
	topo.SetLogLevel(ifs.Info_Level)
	defer reset("TestTransactionReplication")
	nic := topo.VnicByVnetNum(1, 1)

	time.Sleep(time.Second * 5)

	index, _ := replication.ReplicationIndex(ServiceName, 2, nic.Resources())
	if len(index.EndPoints) != 9 {
		Log.Fail(t, "Expected 9 end points, got ", len(index.EndPoints))
		return
	}

	if !doRound(2, 0, t) {
		return
	}

	time.Sleep(time.Second)

	if !doRound(4, 1, t) {
		return
	}
	time.Sleep(time.Second)
}

func doRound(ecount, score int, t *testing.T) bool {
	pb := &testtypes.TestProto{MyString: "test" + strconv.Itoa(score)}
	eg := topo.VnicByVnetNum(2, 1)
	resp := eg.ProximityRequest(ServiceName, 2, ifs.POST, pb)
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
		Log.Fail(t, "Expected count to be ", ecount, " got ", count)
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
