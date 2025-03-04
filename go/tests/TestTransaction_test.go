package tests

import (
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/tests"
	"github.com/saichler/shared/go/types"
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
		ts.Tr = true
	}
	defer func() {
		for _, ts := range tsps {
			ts.Tr = false
		}
	}()

	if !doTransaction(eg3, 1, t) {
		return
	}

	if !doTransaction(eg3, 2, t) {
		return
	}

	if !doTransaction(eg1, 3, t) {
		return
	}
}

func doTransaction(vnic interfaces.IVirtualNetworkInterface, expected int, t *testing.T) bool {
	pb := &tests.TestProto{MyString: "test"}
	resp, err := vnic.Request(types.CastMode_Single, types.Action_POST, 0, "TestProto", pb)
	if err != nil {
		log.Fail(t, err.Error())
		return false
	}

	tr := resp.(*types.Tr)
	if tr.State != types.TrState_Commited {
		log.Fail(t, "transaction state is not commited,", tr.State.String())
		return false
	}

	if tsps["eg1"].PostNumber != expected {
		log.Fail(t, "eg1 Expected post to be ", expected, " but it is ", tsps["eg1"].PostNumber)
	}
	if tsps["eg2"].PostNumber != expected {
		log.Fail(t, "eg2 Expected post to be ", expected, " but it is ", tsps["eg2"].PostNumber)
	}
	if tsps["eg3"].PostNumber != expected {
		log.Fail(t, "eg3 Expected post to be ", expected, " but it is ", tsps["eg3"].PostNumber)
	}
	if tsps["eg4"].PostNumber != expected {
		log.Fail(t, "eg4 Expected post to be ", expected, " but it is ", tsps["eg4"].PostNumber)
	}
	return true
}

func sendTransaction(nic interfaces.IVirtualNetworkInterface, t *testing.T) {
	pb := &tests.TestProto{MyString: "test"}
	_, err := nic.Request(types.CastMode_Single, types.Action_POST, 0, "TestProto", pb)
	if err != nil {
		log.Fail(t, err.Error())
		return
	}
}
