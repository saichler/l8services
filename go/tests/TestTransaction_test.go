package tests

import (
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_servicepoints"
	"github.com/saichler/shared/go/share/workers"
	"github.com/saichler/types/go/testtypes"
	"github.com/saichler/types/go/types"
	"testing"
)

func TestMain(m *testing.M) {
	setup()
	m.Run()
	tear()
}

func TestTransaction(t *testing.T) {
	defer reset("TestTransaction")
	setTransactionMode(0)

	eg2_2 := topo.VnicByVnetNum(2, 2)
	eg1_1 := topo.VnicByVnetNum(1, 1)

	if !doTransaction(types.Action_POST, eg2_2, 1, t, true) {
		return
	}

	if !doTransaction(types.Action_POST, eg2_2, 2, t, true) {
		return
	}

	if !doTransaction(types.Action_POST, eg1_1, 3, t, true) {
		return
	}

}

func TestTransactionPut(t *testing.T) {
	defer reset("TestTransactionPut")
	setTransactionMode(0)

	eg3_2 := topo.VnicByVnetNum(3, 2)

	if !doTransaction(types.Action_PUT, eg3_2, 1, t, true) {
		return
	}
	handler := topo.HandlerByVnetNum(1, 3)
	if handler.PutN() != 1 {
		Log.Fail(t, "Expected 1 put")
	}
}

func TestTransactionGet(t *testing.T) {
	defer reset("TestTransactionGet")
	setTransactionMode(0)

	pb := &testtypes.TestProto{}
	eg3_1 := topo.VnicByVnetNum(3, 1)
	resp := eg3_1.SingleRequest(ServiceName, 0, types.Action_GET, pb)
	if resp.Err() != nil {
		Log.Fail(t, resp.Err().Error())
		return
	}

	handlers := topo.AllHandlers()
	gets := 0
	for _, ts := range handlers {
		gets += ts.GetN()
	}
	if gets != 1 {
		Log.Fail(t, "Expected 1 get ", gets)
		return
	}
}

func TestTransactionPutRollback(t *testing.T) {
	defer reset("TestTransactionPutRollback")
	setTransactionMode(0)
	handler := topo.HandlerByVnetNum(2, 1)
	handler.SetErrorMode(true)

	eg3_1 := topo.VnicByVnetNum(3, 1)
	if !doTransaction(types.Action_PUT, eg3_1, 1, t, false) {
		return
	}

	//2 put, one for the commit and 1 for the rollback
	handler = topo.HandlerByVnetNum(1, 2)
	if handler.PutN() != 2 {
		Log.Fail(t, "Expected 2 put ", handler.PutN())
		return
	}
}

func TestParallel(t *testing.T) {
	defer reset("TestTransaction")
	setTransactionMode(0)

	multi := workers.NewMultiTask()
	add50Transactions(multi, topo.VnicByVnetNum(3, 3))
	add50Transactions(multi, topo.VnicByVnetNum(2, 2))
	add50GetTasks(multi, topo.VnicByVnetNum(3, 3))
	add50GetTasks(multi, topo.VnicByVnetNum(1, 1))

	results := multi.RunAll()

	post := 0
	get := 0

	for _, result := range results {
		tr, ok := result.(*types.Transaction)
		if ok && tr.State == types.TransactionState_Commited {
			post++
		}
		_, ok = result.(*testtypes.TestProto)
		if ok {
			get++
		}
	}
	if post != 100 {
		Log.Fail(t, "expected 100 successful transactions:", post)
		return
	}
	if get != 100 {
		Log.Fail(t, "expected 100 successful gets:", get)
		return
	}
}

func TestTransactionRollback(t *testing.T) {
	defer reset("TestTransactionRollback")
	setTransactionMode(0)
	topo.HandlerByVnetNum(1, 3).SetErrorMode(true)
	eg1_2 := topo.VnicByVnetNum(1, 2)
	if !doTransaction(types.Action_POST, eg1_2, 1, t, false) {
		return
	}

	if !doTransaction(types.Action_POST, eg1_2, 2, t, false) {
		return
	}

	if !doTransaction(types.Action_POST, eg1_2, 3, t, false) {
		return
	}

	dels := 0
	handlers := topo.AllHandlers()
	for _, ts := range handlers {
		dels += ts.DeleteN()
	}
	//Why 21? there are 9 instances of the service, 1 leader and 8 followers.
	//The leader trys to commit on the followers before commiting on it own.
	//One of them fail, hence 7 commited that need to be rollback. The failed one
	//Will auto rollback by itself.
	//We have 3 post times 7 == 21
	if dels != 21 {
		Log.Fail(t, "Expected a rollback on 21 ", dels)
		return
	}
}
