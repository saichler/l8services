package tests

import (
	"strconv"
	"testing"

	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_service"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/testtypes"
)

func TestReplication(t *testing.T) {
	defer reset("TestReplication")
	if !doRound(1, 2, t) {
		return
	}
	if !doRound(1, 4, t) {
		return
	}
	if !doRound(2, 2, t) {
		return
	}
	if !doRound(2, 4, t) {
		return
	}
}

func doRound(index, ecount int, t *testing.T) bool {
	pb := &testtypes.TestProto{MyString: "test" + strconv.Itoa(index)}
	eg := topo.VnicByVnetNum(2, 1)
	resp := eg.ProximityRequest(ServiceName, 2, ifs.POST, pb, 5)

	if resp.Error() != nil {
		Log.Fail(t, resp.Error().Error())
		return false
	}

	count := 0
	handlers := topo.AllRepHandlers()
	for _, ts := range handlers {
		v, ok := ts.PostNReplica().Load(pb.MyString)
		if ok {
			count += v.(int)
		}
	}
	if count != ecount {
		Log.Fail(t, "Expected count 1 to be ", ecount, " got ", count)
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
