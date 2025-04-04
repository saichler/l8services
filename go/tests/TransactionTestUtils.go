package tests

import (
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_servicepoints"
	"github.com/saichler/shared/go/share/workers"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/testtypes"
	"testing"
)

func doTransaction(action common.Action, vnic common.IVirtualNetworkInterface, expected int, t *testing.T, failure bool) bool {
	pb := &testtypes.TestProto{MyString: "test"}
	resp := vnic.SingleRequest(ServiceName, 0, action, pb)
	if resp != nil && resp.Error() != nil {
		Log.Fail(t, resp.Error().Error())
		return false
	}

	tr := resp.Element().(common.ITransaction)
	if tr.State() != common.Commited && failure {
		Log.Fail(t, "transaction state is not commited, ", expected, " ", tr.State().String(), " ", tr.ErrorMessage())
		return false
	}

	if action == common.POST {
		handlers := topo.AllHandlers()
		for _, handler := range handlers {
			if handler.PostN() != expected && failure {
				Log.Fail(t, handler.Name(), " Expected post to be ", expected, " but it is ", handler.PostN())
				return false
			}
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
	resp := this.Vnic.SingleRequest(ServiceName, 0, common.POST, pb)
	if resp != nil && resp.Error() != nil {
		return Log.Error(resp.Error().Error())
	}
	return resp.Element()
}

type GetTask struct {
	Vnic common.IVirtualNetworkInterface
}

func (this *GetTask) Run() interface{} {
	pb := &testtypes.TestProto{MyString: "test"}
	resp := this.Vnic.SingleRequest(ServiceName, 0, common.GET, pb)
	if resp != nil && resp.Error() != nil {
		return Log.Error(resp.Error().Error())
	}
	return resp.Element()
}
