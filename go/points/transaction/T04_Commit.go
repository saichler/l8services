package transaction

import (
	"github.com/saichler/layer8/go/overlay/protocol"
	"github.com/saichler/l8types/go/ifs"
	"time"
)

func (this *ServiceTransactions) commit(msg ifs.IMessage, vnic ifs.IVirtualNetworkInterface) bool {
	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()

	if msg.Tr().State() != ifs.Commit {
		panic("commit: Unexpected transaction state " + msg.Tr().State().String())
	}

	if this.locked == nil {
		msg.Tr().SetState(ifs.Errored)
		msg.Tr().SetErrorMessage("Commit: No pending transaction")
		return false
	}

	if this.locked.Tr().Id() != msg.Tr().Id() {
		msg.Tr().SetState(ifs.Errored)
		msg.Tr().SetErrorMessage("Commit: commit is for another transaction")
		return false
	}

	if this.locked.Tr().State() != ifs.Locked &&
		this.locked.Tr().State() != ifs.Commit { //The state will be commit if the message hit the leader
		msg.Tr().SetErrorMessage("Commit: Transaction is not in locked state " + msg.Tr().State().String())
		msg.Tr().SetState(ifs.Errored)
		return false
	}

	if time.Now().Unix()-this.locked.Tr().StartTime() >= 20 { //@TODO add the timeout
		msg.Tr().SetState(ifs.Errored)
		msg.Tr().SetErrorMessage("Commit: Transaction has timed out")
		return false
	}

	servicePoints := vnic.Resources().Services()
	if msg.Action() == ifs.Notify {
		//_, err := servicePoints.Notify()
	} else {
		pb, err := protocol.ElementsOf(this.locked, vnic.Resources())
		if err != nil {
			msg.Tr().SetState(ifs.Errored)
			msg.Tr().SetErrorMessage("Commit: Protocol Error: " + err.Error())
			return false
		}
		ok := this.setPreCommitObject(msg, vnic)
		if !ok {
			msg.Tr().SetState(ifs.Errored)
			msg.Tr().SetErrorMessage("Commit: Could not set pre-commit object")
			return false
		}

		resp := servicePoints.TransactionHandle(pb, this.locked.Action(), vnic, this.locked)
		if resp != nil && resp.Error() != nil {
			msg.Tr().SetState(ifs.Errored)
			msg.Tr().SetErrorMessage("Commit: Handle Error: " + resp.Error().Error())
			return false
		}
		this.locked.Tr().SetState(ifs.Commited)
	}

	msg.Tr().SetState(ifs.Commited)
	return true
}

func (this *ServiceTransactions) setPreCommitObject(msg ifs.IMessage, vnic ifs.IVirtualNetworkInterface) bool {

	pb, err := protocol.ElementsOf(this.locked, vnic.Resources())
	if err != nil {
		msg.Tr().SetState(ifs.Errored)
		msg.Tr().SetErrorMessage("Pre Commit Object Fetch: Protocol Error: " + err.Error())
		return false
	}

	if msg.Action() == ifs.PUT ||
		msg.Action() == ifs.DELETE ||
		msg.Action() == ifs.PATCH {
		servicePoints := vnic.Resources().Services()
		//Get the object before performing the action so we could rollback
		//if necessary.
		resp := servicePoints.TransactionHandle(pb, ifs.GET, vnic, this.locked)
		if resp != nil && resp.Error() != nil {
			msg.Tr().SetState(ifs.Errored)
			msg.Tr().SetErrorMessage("Pre Commit Object Fetch: Service Point: " + resp.Error().Error())
			return false
		}
		this.preCommitObject = resp
	} else {
		this.preCommitObject = pb
	}
	return true
}
