package states

import (
	"github.com/saichler/l8services/go/services/transaction"
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceTransactions) rollbackInternal(tr *transaction.Transaction) (ifs.TransactionState, string) {

	if tr.Msg().Tr_State() != ifs.Commited {
		panic("T05_Rollback.rollbackInternal: Unexpected transaction state " + tr.Msg().Tr_State().String() + " " + tr.Msg().Tr_Id())
	}

	if this.lockedTrId == "" {
		tr.Msg().SetTr_State(ifs.Errored)
		tr.Msg().SetTr_ErrMsg("T05_Rollback.rollbackInternal: No committed transaction " + tr.Msg().Tr_Id())
		tr.Error(tr.Msg().Tr_ErrMsg())
		return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
	}

	if this.lockedTrId != tr.Msg().Tr_Id() {
		tr.Msg().SetTr_State(ifs.Errored)
		tr.Msg().SetTr_ErrMsg("T05_Rollback.rollbackInternal: commit was for another transaction " + tr.Msg().Tr_Id())
		tr.Error(tr.Msg().Tr_ErrMsg())
		return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
	}

	if tr.Msg().Action() == ifs.Notify {
		//_, err := services.Notify()
	} else {
		this.setRollbackAction(tr.Msg())
		resp := tr.Services().TransactionHandle(tr.PreTrElement(), tr.Msg().Action(), tr.VNic(), tr.Msg())
		if resp != nil && resp.Error() != nil {
			tr.Msg().SetTr_State(ifs.Errored)
			tr.Msg().SetTr_ErrMsg("T05_Rollback.rollbackInternal: Handle Error: " + tr.Msg().Tr_Id() + " " + resp.Error().Error())
			tr.Error(tr.Msg().Tr_ErrMsg())
			return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
		}
	}

	tr.Msg().SetTr_State(ifs.Rollbacked)
	return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
}

func (this *ServiceTransactions) rollbackFollower(msg *ifs.Message, vnic ifs.IVNic) (ifs.TransactionState, string) {
	tr, err := this.getTransaction(msg, vnic)
	if err != nil {
		err = vnic.Resources().Logger().Error("T05_Rollback.rollbackFollower: Transaction ID ", msg.Tr_Id(), " does not exist")
		return ifs.Errored, err.Error()
	}
	this.mtx.Lock()
	defer this.mtx.Unlock()
	return this.rollbackInternal(tr)
}

func (this *ServiceTransactions) setRollbackAction(msg *ifs.Message) {
	switch msg.Action() {
	case ifs.POST:
		msg.SetAction(ifs.DELETE)
	case ifs.DELETE:
		msg.SetAction(ifs.POST)
	case ifs.PUT:
		msg.SetAction(ifs.PUT)
	case ifs.PATCH:
		msg.SetAction(ifs.PUT)
	}
}
