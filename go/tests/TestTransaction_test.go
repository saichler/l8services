package tests

import (
	. "github.com/saichler/shared/go/tests/infra"
	"github.com/saichler/types/go/testtypes"
	"github.com/saichler/types/go/types"
	"testing"
	"time"
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
	_, err := eg3.Transaction(types.Action_GET, 0, "TestProto", pb)
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

	go do50Transactions(eg2)
	go do50Transactions(eg4)
	go do50Gets(eg2)
	go do50Gets(eg3)

	time.Sleep(time.Second)
	Log.Info("Total:", len(trs))
	if len(trs) != 100 {
		Log.Fail(t, "number of commited transactions:", len(trs))
		return
	}
	for _, tr := range trs {
		if tr.State != types.TransactionState_Commited {
			Log.Fail(t, "transaction state:", tr.State)
		}
		Log.Info("Tr:", tr.State.String(), " ", tr.Id, " ", tr.Error)
	}

	if len(gets) != 100 {
		Log.Fail(t, "number of gets:", len(gets))
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
