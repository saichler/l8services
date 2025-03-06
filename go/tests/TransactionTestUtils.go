package tests

import (
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/tests"
	"github.com/saichler/shared/go/types"
	"sync"
	"testing"
)

var trs = make([]*types.Transaction, 0)
var trsMtx = &sync.Mutex{}

func doTransaction(action types.Action, vnic interfaces.IVirtualNetworkInterface, expected int, t *testing.T, failure bool) bool {
	pb := &tests.TestProto{MyString: "test"}
	resp, err := vnic.Transaction(action, 0, "TestProto", pb)
	if err != nil {
		log.Fail(t, err.Error())
		return false
	}

	tr := resp.(*types.Transaction)
	if tr.State != types.TransactionState_Commited && failure {
		log.Fail(t, "transaction state is not commited, ", expected, " ", tr.State.String(), " ", tr.Error)
		return false
	}

	if action == types.Action_POST {
		if tsps["eg1"].PostNumber != expected && failure {
			log.Fail(t, "eg1 Expected post to be ", expected, " but it is ", tsps["eg1"].PostNumber)
		}
		if tsps["eg2"].PostNumber != expected && failure {
			log.Fail(t, "eg2 Expected post to be ", expected, " but it is ", tsps["eg2"].PostNumber)
		}
		if tsps["eg3"].PostNumber != expected && failure {
			log.Fail(t, "eg3 Expected post to be ", expected, " but it is ", tsps["eg3"].PostNumber)
		}
		if tsps["eg4"].PostNumber != expected && failure {
			log.Fail(t, "eg4 Expected post to be ", expected, " but it is ", tsps["eg4"].PostNumber)
		}
	}
	return true
}

func do50Transactions(nic interfaces.IVirtualNetworkInterface) bool {
	for i := 0; i < 50; i++ {
		sendTransaction(nic)
	}
	return true
}

func sendTransaction(nic interfaces.IVirtualNetworkInterface) {
	pb := &tests.TestProto{MyString: "test"}
	resp, err := nic.Request(types.CastMode_Single, types.Action_POST, 0, "TestProto", pb)
	if err != nil {
		log.Error(err.Error())
		return
	}

	tr := resp.(*types.Transaction)
	trsMtx.Lock()
	defer trsMtx.Unlock()
	trs = append(trs, tr)
}
