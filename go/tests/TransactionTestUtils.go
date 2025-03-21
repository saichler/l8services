package tests

import (
	. "github.com/saichler/shared/go/tests/infra"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/testtypes"
	"github.com/saichler/types/go/types"
	"sync"
	"testing"
)

var trs = make([]*types.Transaction, 0)
var gets = make([]*testtypes.TestProto, 0)

var trsMtx = &sync.Mutex{}

func doTransaction(action types.Action, vnic common.IVirtualNetworkInterface, expected int, t *testing.T, failure bool) bool {
	pb := &testtypes.TestProto{MyString: "test"}
	resp, err := vnic.SingleRequest(ServiceName, 0, action, pb)
	if err != nil {
		Log.Fail(t, err.Error())
		return false
	}

	tr := resp.(*types.Transaction)
	if tr.State != types.TransactionState_Commited && failure {
		Log.Fail(t, "transaction state is not commited, ", expected, " ", tr.State.String(), " ", tr.Error)
		return false
	}

	if action == types.Action_POST {
		if tsps["eg1"].PostN() != expected && failure {
			Log.Fail(t, "eg1 Expected post to be ", expected, " but it is ", tsps["eg1"].PostN())
		}
		if tsps["eg2"].PostN() != expected && failure {
			Log.Fail(t, "eg2 Expected post to be ", expected, " but it is ", tsps["eg2"].PostN())
		}
		if tsps["eg3"].PostN() != expected && failure {
			Log.Fail(t, "eg3 Expected post to be ", expected, " but it is ", tsps["eg3"].PostN())
		}
		if tsps["eg4"].PostN() != expected && failure {
			Log.Fail(t, "eg4 Expected post to be ", expected, " but it is ", tsps["eg4"].PostN())
		}
	}
	return true
}

func do50Gets(nic common.IVirtualNetworkInterface) bool {
	for i := 0; i < 50; i++ {
		go sendGet(nic)
	}
	return true
}

func do50Transactions(nic common.IVirtualNetworkInterface) bool {
	for i := 0; i < 50; i++ {
		go sendTransaction(nic)
	}
	return true
}

func sendTransaction(nic common.IVirtualNetworkInterface) {
	pb := &testtypes.TestProto{MyString: "test"}
	resp, err := nic.SingleRequest(ServiceName, 0, types.Action_POST, pb)
	if err != nil {
		Log.Error(err.Error())
		return
	}

	tr := resp.(*types.Transaction)
	trsMtx.Lock()
	defer trsMtx.Unlock()
	trs = append(trs, tr)
}

func sendGet(nic common.IVirtualNetworkInterface) {
	pb := &testtypes.TestProto{MyString: "test"}
	resp, err := nic.SingleRequest(ServiceName, 0, types.Action_GET, pb)
	if err != nil {
		Log.Error(err.Error())
		return
	}

	tr := resp.(*testtypes.TestProto)
	trsMtx.Lock()
	defer trsMtx.Unlock()
	gets = append(gets, tr)
}
