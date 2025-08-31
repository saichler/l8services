package states

import (
	"strconv"

	"github.com/saichler/l8services/go/services/transaction"
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceTransactions) lockInternal(tr *transaction.Transaction) (ifs.TransactionState, string) {
	if tr.Msg().Tr_State() != ifs.Created {
		panic("T03_Lock.internalLock: Unexpected transaction state " + tr.Msg().Tr_State().String() + " " + tr.Msg().Tr_Id() + " " + tr.Msg().Tr_State().String())
	}

	if this.lockedTrId == "" {
		this.lockedTrId = tr.Msg().Tr_Id()
		tr.Msg().SetTr_State(ifs.Locked)
		tr.Debug("T03_Lock.internalLock: Transaction state is locked ", tr.Msg().Tr_Id())
		return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
	}

	tr.Msg().SetTr_State(ifs.LockFailed)
	tr.Msg().SetTr_ErrMsg("T03_Lock.internalLock: Failed to lock : " + tr.Msg().ServiceName() + " area " +
		strconv.Itoa(int(tr.Msg().ServiceArea())) + " " + tr.Msg().Tr_Id())
	tr.Error(tr.Msg().Tr_ErrMsg())
	return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
}

func (this *ServiceTransactions) lockFollower(msg *ifs.Message, vnic ifs.IVNic) (ifs.TransactionState, string) {
	tr, err := this.getTransaction(msg, vnic)
	if err != nil {
		err = vnic.Resources().Logger().Error("T03_Lock.lockFollower: Transaction ID ", msg.Tr_Id(), " does not exist")
		return ifs.Errored, err.Error()
	}
	this.mtx.Lock()
	defer this.mtx.Unlock()
	return this.lockInternal(tr)
}
