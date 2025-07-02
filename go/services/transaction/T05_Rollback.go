package transaction

import (
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceTransactions) rollback(msg *ifs.Message, vnic ifs.IVNic) bool {
	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()

	if msg.Tr_State() != ifs.Rollback {
		panic("commit: Unexpected transaction state " + msg.Tr_State().String())
	}

	if this.locked == nil {
		msg.SetTr_State(ifs.Errored)
		msg.SetTr_ErrMsg("Rollback: No committed transaction")
		return false
	}

	if this.locked.Tr_Id() != msg.Tr_Id() {
		msg.SetTr_State(ifs.Errored)
		msg.SetTr_ErrMsg("Rollback: commit was for another transaction")
		return false
	}

	if this.locked.Tr_State() != ifs.Commited {
		msg.SetTr_ErrMsg("Rollback: Transaction is not in committed state " + msg.Tr_State().String())
		msg.SetTr_State(ifs.Errored)
		return false
	}

	services := vnic.Resources().Services()
	if msg.Action() == ifs.Notify {
		//_, err := services.Notify()
	} else {
		this.setRollbackAction(msg)
		resp := services.TransactionHandle(this.preCommitObject, this.locked.Action(), vnic, this.locked)
		if resp != nil && resp.Error() != nil {
			msg.SetTr_State(ifs.Errored)
			msg.SetTr_ErrMsg("Rollback: Handle Error: " + resp.Error().Error())
			return false
		}
	}

	msg.SetTr_State(ifs.Rollbacked)
	return true
}

func (this *ServiceTransactions) setRollbackAction(msg *ifs.Message) {
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
