package states

import (
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceTransactions) rollbackInternal(msg *ifs.Message) ifs.IElements {

	if msg.Action() == ifs.Notify {
		return nil
	}

	this.setRollbackAction(msg)

	this.preCommitMtx.Lock()
	defer this.preCommitMtx.Unlock()

	elem := this.preCommitObject(msg)
	resp := this.nic.Resources().Services().TransactionHandle(elem, msg.Action(), msg, this.nic)
	if resp != nil && resp.Error() != nil {
		msg.SetTr_State(ifs.Failed)
		msg.SetTr_ErrMsg("T05_Rollback.rollbackInternal: Handle Error: " + msg.Tr_Id() + " " + resp.Error().Error())
		delete(this.preCommit, msg.Tr_Id())
		return L8TransactionFor(msg)
	}
	delete(this.preCommit, msg.Tr_Id())
	return L8TransactionFor(msg)
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

func (this *ServiceTransactions) preCommitObject(msg *ifs.Message) ifs.IElements {
	item := this.preCommit[msg.Tr_Id()]
	elem, ok := item.(ifs.IElements)
	if ok {
		return elem
	}
	return object.New(nil, item)
}
