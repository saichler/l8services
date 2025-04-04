package transaction

import (
	"github.com/saichler/types/go/common"
)

func (this *ServiceTransactions) rollback(msg common.IMessage, vnic common.IVirtualNetworkInterface) bool {
	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()

	if msg.Tr().State() != common.Rollback {
		panic("commit: Unexpected transaction state " + msg.Tr().State().String())
	}

	if this.locked == nil {
		msg.Tr().SetState(common.Errored)
		msg.Tr().SetErrorMessage("Rollback: No committed transaction")
		return false
	}

	if this.locked.Tr().Id() != msg.Tr().Id() {
		msg.Tr().SetState(common.Errored)
		msg.Tr().SetErrorMessage("Rollback: commit was for another transaction")
		return false
	}

	if this.locked.Tr().State() != common.Commited {
		msg.Tr().SetErrorMessage("Rollback: Transaction is not in committed state " + msg.Tr().State().String())
		msg.Tr().SetState(common.Errored)
		return false
	}

	servicePoints := vnic.Resources().ServicePoints()
	if msg.Action() == common.Notify {
		//_, err := servicePoints.Notify()
	} else {
		this.setRollbackAction(msg)
		resp := servicePoints.Handle(this.preCommitObject, this.locked.Action(), vnic, this.locked, true)
		if resp != nil && resp.Error() != nil {
			msg.Tr().SetState(common.Errored)
			msg.Tr().SetErrorMessage("Rollback: Handle Error: " + resp.Error().Error())
			return false
		}
	}

	msg.Tr().SetState(common.Rollbacked)
	return true
}

func (this *ServiceTransactions) setRollbackAction(msg common.IMessage) {
	switch msg.Action() {
	case common.POST:
		this.locked.SetAction(common.DELETE)
	case common.DELETE:
		this.locked.SetAction(common.POST)
	case common.PUT:
		this.locked.SetAction(common.PUT)
	case common.PATCH:
		this.locked.SetAction(common.PUT)
	}
}
