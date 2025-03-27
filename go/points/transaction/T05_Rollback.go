package transaction

import (
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
)

func (this *ServiceTransactions) rollback(msg *types.Message, vnic common.IVirtualNetworkInterface) bool {
	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()

	if msg.Tr.State != types.TransactionState_Rollback {
		panic("commit: Unexpected transaction state " + msg.Tr.State.String())
	}

	if this.locked == nil {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Rollback: No committed transaction"
		return false
	}

	if this.locked.Tr.Id != msg.Tr.Id {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Rollback: commit was for another transaction"
		return false
	}

	if this.locked.Tr.State != types.TransactionState_Commited {
		msg.Tr.Error = "Rollback: Transaction is not in committed state " + msg.Tr.State.String()
		msg.Tr.State = types.TransactionState_Errored
		return false
	}

	servicePoints := vnic.Resources().ServicePoints()
	if msg.Action == types.Action_Notify {
		//_, err := servicePoints.Notify()
	} else {
		this.setRollbackAction(msg)
		resp := servicePoints.Handle(this.preCommitObject, this.locked.Action, vnic, this.locked, true)
		if resp.Err() != nil {
			msg.Tr.State = types.TransactionState_Errored
			msg.Tr.Error = "Rollback: Handle Error: " + resp.Err().Error()
			return false
		}
	}

	msg.Tr.State = types.TransactionState_Rollbacked
	return true
}

func (this *ServiceTransactions) setRollbackAction(msg *types.Message) {
	switch msg.Action {
	case types.Action_POST:
		this.locked.Action = types.Action_DELETE
	case types.Action_DELETE:
		this.locked.Action = types.Action_POST
	case types.Action_PUT:
		this.locked.Action = types.Action_PUT
	case types.Action_PATCH:
		this.locked.Action = types.Action_PUT
	}
}
