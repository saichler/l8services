package tests

import (
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_service"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/testtypes"
	"github.com/saichler/l8types/go/types"
	"github.com/saichler/l8utils/go/utils/workers"
	"testing"
)

func doTransaction(action ifs.Action, vnic ifs.IVNic, expected int, t *testing.T, failure bool) bool {
	pb := &testtypes.TestProto{MyString: "test"}
	resp := vnic.ProximityRequest(ServiceName, 1, action, pb)
	if resp != nil && resp.Error() != nil {
		Log.Fail(t, resp.Error().Error())
		return false
	}

	tr := resp.Element().(*types.Transaction)
	if tr.State != int32(ifs.Commited) && failure {
		Log.Fail(t, "transaction state is not commited, ", expected, " ", ifs.TransactionState(tr.State), " ", tr.ErrMsg)
		return false
	}

	if action == ifs.POST {
		handlers := topo.AllTrHandlers()
		for _, handler := range handlers {
			if handler.PostN() != expected && failure {
				Log.Fail(t, handler.Name(), " Expected post to be ", expected, " but it is ", handler.PostN())
				return false
			}
		}
	}
	return true
}

func add50GetTasks(multiTask *workers.MultiTask, vnic ifs.IVNic) {
	for i := 0; i < 50; i++ {
		multiTask.AddTask(&GetTask{Vnic: vnic})
	}
}

func add50Transactions(multiTask *workers.MultiTask, vnic ifs.IVNic) bool {
	for i := 0; i < 50; i++ {
		multiTask.AddTask(&PostTask{Vnic: vnic})
	}
	return true
}

type PostTask struct {
	Vnic ifs.IVNic
}

func (this *PostTask) Run() interface{} {
	pb := &testtypes.TestProto{MyString: "test"}
	resp := this.Vnic.ProximityRequest(ServiceName, 1, ifs.POST, pb)
	if resp != nil && resp.Error() != nil {
		return Log.Error(resp.Error().Error())
	}
	return resp.Element()
}

type GetTask struct {
	Vnic ifs.IVNic
}

func (this *GetTask) Run() interface{} {
	pb := &testtypes.TestProto{MyString: "test"}
	resp := this.Vnic.ProximityRequest(ServiceName, 1, ifs.GET, pb)
	if resp != nil && resp.Error() != nil {
		return Log.Error(resp.Error().Error())
	}
	return resp.Element()
}
