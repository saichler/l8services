package tests

import (
	"github.com/saichler/shared/go/share/workers"
	. "github.com/saichler/shared/go/tests/infra"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/testtypes"
	"github.com/saichler/types/go/types"
	"testing"
)

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

func add50GetTasks(multiTask *workers.MultiTask, vnic common.IVirtualNetworkInterface) {
	for i := 0; i < 50; i++ {
		multiTask.AddTask(&GetTask{Vnic: vnic})
	}
}

func add50Transactions(multiTask *workers.MultiTask, vnic common.IVirtualNetworkInterface) bool {
	for i := 0; i < 50; i++ {
		multiTask.AddTask(&PostTask{Vnic: vnic})
	}
	return true
}

type PostTask struct {
	Vnic common.IVirtualNetworkInterface
}

func (this *PostTask) Run() interface{} {
	pb := &testtypes.TestProto{MyString: "test"}
	resp, err := this.Vnic.SingleRequest(ServiceName, 0, types.Action_POST, pb)
	if err != nil {
		return Log.Error(err.Error())
	}
	return resp
}

type GetTask struct {
	Vnic common.IVirtualNetworkInterface
}

func (this *GetTask) Run() interface{} {
	pb := &testtypes.TestProto{MyString: "test"}
	resp, err := this.Vnic.SingleRequest(ServiceName, 0, types.Action_GET, pb)
	if err != nil {
		return Log.Error(err.Error())
	}
	return resp
}
