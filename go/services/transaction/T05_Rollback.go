package transaction

import (
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceTransactions) rollback(msg ifs.IMessage, vnic ifs.IVNic) bool {
	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()

	if msg.Tr().State() != ifs.Rollback {
		panic("commit: Unexpected transaction state " + msg.Tr().State().String())
	}

	if this.locked == nil {
		msg.Tr().SetState(ifs.Errored)
		msg.Tr().SetErrorMessage("Rollback: No committed transaction")
		return false
	}

	if this.locked.Tr().Id() != msg.Tr().Id() {
		msg.Tr().SetState(ifs.Errored)
		msg.Tr().SetErrorMessage("Rollback: commit was for another transaction")
		return false
	}

	if this.locked.Tr().State() != ifs.Commited {
		msg.Tr().SetErrorMessage("Rollback: Transaction is not in committed state " + msg.Tr().State().String())
		msg.Tr().SetState(ifs.Errored)
		return false
	}

	services := vnic.Resources().Services()
	if msg.Action() == ifs.Notify {
		//_, err := services.Notify()
	} else {
		this.setRollbackAction(msg)
		resp := services.TransactionHandle(this.preCommitObject, this.locked.Action(), vnic, this.locked)
		if resp != nil && resp.Error() != nil {
			msg.Tr().SetState(ifs.Errored)
			msg.Tr().SetErrorMessage("Rollback: Handle Error: " + resp.Error().Error())
			return false
		}
	}

	msg.Tr().SetState(ifs.Rollbacked)
	return true
}

func (this *ServiceTransactions) setRollbackAction(msg ifs.IMessage) {
	switch msg.Action() {
	case ifs.POST:
		this.locked.SetAction(ifs.DELETE)
	case ifs.DELETE:
		this.locked.SetAction(ifs.POST)
	case ifs.PUT:
		this.locked.SetAction(ifs.PUT)
	case ifs.PATCH:
		this.locked.SetAction(ifs.PUT)
	}
}
