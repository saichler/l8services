package states

import (
	"time"

	"github.com/saichler/l8services/go/services/transaction"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/layer8/go/overlay/protocol"
)

func (this *ServiceTransactions) commitInternal(tr *transaction.Transaction) (ifs.TransactionState, string) {

	if tr.Msg().Tr_State() != ifs.Commit {
		panic("T04_Commit.commitInternal: Unexpected transaction state " + tr.Msg().Tr_State().String())
	}

	if this.lockedTrId == "" {
		tr.Msg().SetTr_State(ifs.Errored)
		tr.Msg().SetTr_ErrMsg("T04_Commit.commitInternal: No pending transaction")
		tr.Error(tr.Msg().Tr_ErrMsg())
		return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
	}

	if this.lockedTrId != tr.Msg().Tr_Id() {
		tr.Msg().SetTr_State(ifs.Errored)
		tr.Msg().SetTr_ErrMsg("T04_Commit.commitInternal: commit is for another transaction")
		tr.Error(tr.Msg().Tr_ErrMsg())
		return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
	}

	if time.Now().Unix()-tr.Msg().Tr_StartTime() >= 20 { //@TODO add the timeout
		tr.Msg().SetTr_State(ifs.Errored)
		tr.Msg().SetTr_ErrMsg("T04_Commit.commitInternal: Transaction has timed out " + tr.Msg().Tr_Id())
		tr.Error(tr.Msg().Tr_ErrMsg())
		return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
	}

	if tr.Msg().Action() == ifs.Notify {
		//_, err := services.Notify()
	} else {
		pb, err := protocol.ElementsOf(tr.Msg(), tr.Resources())
		if err != nil {
			tr.Msg().SetTr_State(ifs.Errored)
			tr.Msg().SetTr_ErrMsg("T04_Commit.commitInternal: Protocol Error: " + tr.Msg().Tr_Id() + " " + err.Error())
			tr.Error(tr.Msg().Tr_ErrMsg())
			return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
		}
		state, _ := this.setPreCommitObject(tr)
		if state == ifs.Errored {
			tr.Msg().SetTr_State(ifs.Errored)
			tr.Msg().SetTr_ErrMsg("T04_Commit.commitInternal: Could not set pre-commit object " + tr.Msg().Tr_Id())
			tr.Error(tr.Msg().Tr_ErrMsg())
			return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
		}

		tr.Debug("T04_Commit.commitInternal: Before Transaction Handle ", tr.Msg().Tr_Id())
		resp := tr.Services().TransactionHandle(pb, tr.Msg().Action(), tr.VNic(), tr.Msg())
		if resp != nil && resp.Error() != nil {
			tr.Msg().SetTr_State(ifs.Errored)
			tr.Msg().SetTr_ErrMsg("T04_Commit.commitInternal: Handle Error: " + tr.Msg().Tr_Id() + " " + resp.Error().Error())
			tr.Error(tr.Msg().Tr_ErrMsg())
			return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
		}
		tr.Debug("T04_Commit.commitInternal: Before Setting Transaction as commited ", tr.Msg().Tr_Id())
		tr.Msg().SetTr_State(ifs.Commited)
	}

	tr.Msg().SetTr_State(ifs.Commited)
	tr.Debug("T04_Commit.commitInternal: Transaction set as commited ", tr.Msg().Tr_Id())
	return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
}

func (this *ServiceTransactions) commitFollower(msg *ifs.Message, vnic ifs.IVNic) (ifs.TransactionState, string) {
	tr, err := this.getTransaction(msg, vnic)
	if err != nil {
		err = vnic.Resources().Logger().Error("T04_Commit.commitFollower: ", err.Error())
		return ifs.Errored, err.Error()
	}
	this.mtx.Lock()
	defer this.mtx.Unlock()
	tr.Msg().SetTr_State(msg.Tr_State())
	return this.commitInternal(tr)
}

func (this *ServiceTransactions) setPreCommitObject(tr *transaction.Transaction) (ifs.TransactionState, string) {

	pb, err := protocol.ElementsOf(tr.Msg(), tr.Resources())
	if err != nil {
		tr.Msg().SetTr_State(ifs.Errored)
		tr.Msg().SetTr_ErrMsg("T04_Commit.setPreCommitObject: Pre Commit Object Fetch: Protocol Error: " + tr.Msg().Tr_Id() + " " + err.Error())
		tr.Error(tr.Msg().Tr_ErrMsg())
		return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
	}

	if tr.Msg().Action() == ifs.PUT ||
		tr.Msg().Action() == ifs.DELETE ||
		tr.Msg().Action() == ifs.PATCH {
		//Get the object before performing the action so we could rollback
		//if necessary.
		resp := tr.Services().TransactionHandle(pb, ifs.GET, tr.VNic(), tr.Msg())
		if resp != nil && resp.Error() != nil {
			tr.Msg().SetTr_State(ifs.Errored)
			tr.Msg().SetTr_ErrMsg("T04_Commit.setPreCommitObject: Pre Commit Object Fetch: Service : " + tr.Msg().Tr_Id() + " " + resp.Error().Error())
			tr.Error(tr.Msg().Tr_ErrMsg())
			return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
		}
		tr.SetPreTrElement(resp)
	} else {
		tr.SetPreTrElement(pb)
	}
	return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
}
