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

func TestMain(m *testing.M) {
	setup()
	m.Run()
	tear()
}

func TestTransaction(t *testing.T) {
	defer reset("TestTransaction")

	eg2_2 := topo.VnicByVnetNum(2, 2)
	eg1_1 := topo.VnicByVnetNum(1, 1)

	if !doTransaction(ifs.POST, eg2_2, 1, t, true) {
		return
	}

	if !doTransaction(ifs.POST, eg2_2, 2, t, true) {
		return
	}

	if !doTransaction(ifs.POST, eg1_1, 3, t, true) {
		return
	}

}

func TestTransactionPut(t *testing.T) {
	defer reset("TestTransactionPut")

	eg3_2 := topo.VnicByVnetNum(3, 2)

	if !doTransaction(ifs.PUT, eg3_2, 1, t, true) {
		return
	}
	handler := topo.TrHandlerByVnetNum(1, 3)
	if handler.PutN() != 1 {
		Log.Fail(t, "Expected 1 put")
	}
}

func TestTransactionGet(t *testing.T) {
	defer reset("TestTransactionGet")

	pb := &testtypes.TestProto{MyString: "test"}
	eg3_1 := topo.VnicByVnetNum(3, 1)
	resp := eg3_1.SingleRequest(ServiceName, 1, ifs.GET, pb)
	if resp.Error() != nil {
		Log.Fail(t, resp.Error().Error())
		return
	}

	handlers := topo.AllTrHandlers()
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
	handler := topo.TrHandlerByVnetNum(2, 1)
	handler.SetErrorMode(true)

	eg3_1 := topo.VnicByVnetNum(3, 1)
	if !doTransaction(ifs.PUT, eg3_1, 1, t, false) {
		return
	}

	//2 put, one for the commit and 1 for the rollback
	handler = topo.TrHandlerByVnetNum(1, 2)
	if handler.PutN() != 2 {
		Log.Fail(t, "Expected 2 put ", handler.PutN())
		return
	}
}

func TestParallel(t *testing.T) {
	topo.SetLogLevel(ifs.Error_Level)
	Log.SetLogLevel(ifs.Error_Level)
	defer reset("TestTransaction")

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
		if ok && tr.State == int32(ifs.Commited) {
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
	topo.TrHandlerByVnetNum(1, 3).SetErrorMode(true)
	eg1_2 := topo.VnicByVnetNum(1, 2)
	if !doTransaction(ifs.POST, eg1_2, 1, t, false) {
		return
	}

	if !doTransaction(ifs.POST, eg1_2, 2, t, false) {
		return
	}

	if !doTransaction(ifs.POST, eg1_2, 3, t, false) {
		return
	}

	dels := 0
	handlers := topo.AllTrHandlers()
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
