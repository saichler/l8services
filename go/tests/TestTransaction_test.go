package tests

import (
	"github.com/saichler/shared/go/share/workers"
	. "github.com/saichler/shared/go/tests/infra"
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
	for _, ts := range tsps {
		ts.SetTr(true)
	}

	if !doTransaction(types.Action_POST, eg3, 1, t, true) {
		return
	}

	if !doTransaction(types.Action_POST, eg3, 2, t, true) {
		return
	}

	if !doTransaction(types.Action_POST, eg1, 3, t, true) {
		return
	}

}

func TestTransactionPut(t *testing.T) {
	defer reset("TestTransactionPut")
	for _, ts := range tsps {
		ts.SetTr(true)
	}

	if !doTransaction(types.Action_PUT, eg3, 1, t, true) {
		return
	}
	if tsps["eg2"].PutN() != 1 {
		Log.Fail(t, "Expected 1 put")
	}
}

func TestTransactionGet(t *testing.T) {
	defer reset("TestTransactionGet")
	for _, ts := range tsps {
		ts.SetTr(true)
	}

	pb := &testtypes.TestProto{}
	_, err := eg3.SingleRequest(ServiceName, 0, types.Action_GET, pb)
	if err != nil {
		Log.Fail(t, err.Error())
		return
	}

	if tsps["eg2"].GetN() != 0 {
		Log.Fail(t, "Expected 0 Get")
	}
	if tsps["eg3"].GetN() != 1 {
		Log.Fail(t, "Expected 1 Get")
	}
}

func TestTransactionPutRollback(t *testing.T) {
	defer reset("TestTransactionPutRollback")
	for _, ts := range tsps {
		ts.SetTr(true)
		if ts.Name() == "eg2" {
			ts.SetErrorMode(true)
		}
	}

	if !doTransaction(types.Action_PUT, eg3, 1, t, false) {
		return
	}
	//2 put, one for the commit and 1 for the rollback
	if tsps["eg4"].PutN() != 2 {
		Log.Fail(t, "Expected 2 put")
	}
}

func TestParallel(t *testing.T) {
	defer reset("TestTransaction")
	for _, ts := range tsps {
		ts.SetTr(true)
	}

	multi := workers.NewMultiTask()
	add50Transactions(multi, eg2)
	add50Transactions(multi, eg4)
	add50GetTasks(multi, eg2)
	add50GetTasks(multi, eg3)

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
		Log.Fail(t, "expected 100 successful transactions")
		return
	}
	if get != 100 {
		Log.Fail(t, "expected 100 successful gets")
		return
	}
}

func TestTransactionRollback(t *testing.T) {
	defer reset("TestTransactionRollback")
	for key, ts := range tsps {
		ts.SetTr(true)
		if key == "eg2" {
			ts.SetErrorMode(true)
		}
	}

	if !doTransaction(types.Action_POST, eg3, 1, t, false) {
		return
	}

	if !doTransaction(types.Action_POST, eg3, 2, t, false) {
		return
	}

	if !doTransaction(types.Action_POST, eg1, 3, t, false) {
		return
	}

	found := false
	for _, ts := range tsps {
		if ts.DeleteN() > 0 {
			found = true
		}
	}
	if !found {
		Log.Fail(t, "Expected a rollback")
	}
}
