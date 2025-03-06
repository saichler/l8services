package tests

import (
	"github.com/saichler/shared/go/types"
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
		ts.Tr = true
	}
	defer func() {
		for _, ts := range tsps {
			ts.Tr = false
		}
	}()

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
	defer reset("TestTransaction")
	for _, ts := range tsps {
		ts.Tr = true
	}
	defer func() {
		for _, ts := range tsps {
			ts.Tr = false
		}
	}()

	if !doTransaction(types.Action_PUT, eg3, 1, t, true) {
		return
	}
	if tsps["eg2"].PutNumber != 1 {
		log.Fail(t, "Expected 1 put")
	}
}

func TestTransactionPutRollback(t *testing.T) {
	defer reset("TestTransaction")
	for _, ts := range tsps {
		ts.Tr = true
		if ts.Name == "eg2" {
			ts.ErrorMode = true
		}
	}

	if !doTransaction(types.Action_PUT, eg3, 1, t, false) {
		return
	}
	//2 put, one for the commit and 1 for the rollback
	if tsps["eg4"].PutNumber != 2 {
		log.Fail(t, "Expected 2 put")
	}
}

func TestParallel(t *testing.T) {
	defer reset("TestTransaction")
	for _, ts := range tsps {
		ts.Tr = true
	}
	defer func() {
		for _, ts := range tsps {
			ts.Tr = false
		}
	}()
	go do50Transactions(eg2)
	go do50Transactions(eg4)
	time.Sleep(time.Second)
	log.Info("Total:", len(trs))
	if len(trs) != 100 {
		log.Fail(t, "number of commited transactions:", len(trs))
		return
	}
	for _, tr := range trs {
		if tr.State != types.TransactionState_Commited {
			log.Fail(t, "transaction state:", tr.State)
		}
		log.Info("Tr:", tr.State.String(), " ", tr.Id, " ", tr.Error)
	}
}

func TestTransactionRollback(t *testing.T) {
	defer reset("TestTransactionRollback")
	for key, ts := range tsps {
		ts.Tr = true
		if key == "eg2" {
			ts.ErrorMode = true
		}
	}
	defer func() {
		for _, ts := range tsps {
			ts.Tr = false
		}
	}()

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
		if ts.DeleteNumber > 0 {
			found = true
		}
	}
	if !found {
		log.Fail(t, "Expected a rollback")
	}
}
