package transaction

import (
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/layer8/go/overlay/protocol"
	"time"
)

func (this *ServiceTransactions) commit(msg *ifs.Message, vnic ifs.IVNic) bool {
	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()

	if msg.Tr_State() != ifs.Commit {
		panic("commit: Unexpected transaction state " + msg.Tr_State().String())
	}

	if this.locked == nil {
		msg.SetTr_State(ifs.Errored)
		msg.SetTr_ErrMsg("Commit: No pending transaction")
		return false
	}

	if this.locked.Tr_Id() != msg.Tr_Id() {
		msg.SetTr_State(ifs.Errored)
		msg.SetTr_ErrMsg("Commit: commit is for another transaction")
		return false
	}

	if this.locked.Tr_State() != ifs.Locked &&
		this.locked.Tr_State() != ifs.Commit { //The state will be commit if the message hit the leader
		msg.SetTr_ErrMsg("Commit: Transaction is not in locked state " + msg.Tr_State().String())
		msg.SetTr_State(ifs.Errored)
		return false
	}

	if time.Now().Unix()-this.locked.Tr_StartTime() >= 20 { //@TODO add the timeout
		msg.SetTr_State(ifs.Errored)
		msg.SetTr_ErrMsg("Commit: Transaction has timed out")
		return false
	}

	services := vnic.Resources().Services()
	if msg.Action() == ifs.Notify {
		//_, err := services.Notify()
	} else {
		pb, err := protocol.ElementsOf(this.locked, vnic.Resources())
		if err != nil {
			msg.SetTr_State(ifs.Errored)
			msg.SetTr_ErrMsg("Commit: Protocol Error: " + err.Error())
			return false
		}
		ok := this.setPreCommitObject(msg, vnic)
		if !ok {
			msg.SetTr_State(ifs.Errored)
			msg.SetTr_ErrMsg("Commit: Could not set pre-commit object")
			return false
		}

		resp := services.TransactionHandle(pb, this.locked.Action(), vnic, this.locked)
		if resp != nil && resp.Error() != nil {
			msg.SetTr_State(ifs.Errored)
			msg.SetTr_ErrMsg("Commit: Handle Error: " + resp.Error().Error())
			return false
		}
		this.locked.SetTr_State(ifs.Commited)
	}

	msg.SetTr_State(ifs.Commited)
	return true
}

func (this *ServiceTransactions) setPreCommitObject(msg *ifs.Message, vnic ifs.IVNic) bool {

	pb, err := protocol.ElementsOf(this.locked, vnic.Resources())
	if err != nil {
		msg.SetTr_State(ifs.Errored)
		msg.SetTr_ErrMsg("Pre Commit Object Fetch: Protocol Error: " + err.Error())
		return false
	}

	if msg.Action() == ifs.PUT ||
		msg.Action() == ifs.DELETE ||
		msg.Action() == ifs.PATCH {
		services := vnic.Resources().Services()
		//Get the object before performing the action so we could rollback
		//if necessary.
		resp := services.TransactionHandle(pb, ifs.GET, vnic, this.locked)
		if resp != nil && resp.Error() != nil {
			msg.SetTr_State(ifs.Errored)
			msg.SetTr_ErrMsg("Pre Commit Object Fetch: Service : " + resp.Error().Error())
			return false
		}
		this.preCommitObject = resp
	} else {
		this.preCommitObject = pb
	}
	return true
}
