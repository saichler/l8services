package tests

import (
	"github.com/saichler/layer8/go/overlay/health"
	. "github.com/saichler/shared/go/tests/infra"
	"github.com/saichler/types/go/testtypes"
	"github.com/saichler/types/go/types"
	"testing"
)

func TestTransactionReplication(t *testing.T) {
	defer reset("TestTransactionReplication")
	for _, ts := range tsps {
		ts.SetTr(true)
		ts.SetReplicationCount(2)
	}

	if !doRound(2, 0, t) {
		return
	}

	if !doRound(4, 1, t) {
		return
	}
}

func doRound(ecount, score int, t *testing.T) bool {
	pb := &testtypes.TestProto{MyString: "test"}
	_, err := eg3.Transaction(types.Action_POST, 0, "TestProto", pb)
	if err != nil {
		Log.Fail(t, err.Error())
		return false
	}

	count := 0
	for _, ts := range tsps {
		count += ts.PostN()
	}
	if count != ecount {
		Log.Fail(t, "Expected count to be ", ecount, "got ", count)
		return false
	}

	hp := health.Health(eg3.Resources())
	rep := hp.ReplicasFor("TestProto", 0, 2)
	for _, r := range rep {
		if int(r) != score {
			Log.Fail(t, "Expected score to be ", score, "got ", r)
			return false
		}
	}
	return true
}
