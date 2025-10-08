package states

import (
	"github.com/saichler/l8bus/go/overlay/protocol"
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceTransactions) commitInternal(msg *ifs.Message) ifs.IElements {
	if msg.Action() == ifs.Notify {
		//_, err := services.Notify()
		return nil
	}

	pb, err := protocol.ElementsOf(msg, this.nic.Resources())
	if err != nil {
		msg.SetTr_State(ifs.Failed)
		msg.SetTr_ErrMsg("T04_Commit.commitInternal: Protocol Error: " + msg.Tr_Id() + " " + err.Error())
		this.nic.Resources().Logger().Debug(msg.Tr_Id() + " " + err.Error())
		return L8TransactionFor(msg)
	}

	err = this.setPreCommitObject(msg)
	if err != nil {
		msg.SetTr_State(ifs.Failed)
		msg.SetTr_ErrMsg(err.Error())
		this.nic.Resources().Logger().Debug(msg.Tr_Id() + " " + err.Error())
		return L8TransactionFor(msg)
	}

	this.nic.Resources().Logger().Debug("T04_Commit.commitInternal: Before Transaction Handle ", msg.Tr_Id())
	resp := this.nic.Resources().Services().TransactionHandle(pb, msg.Action(), this.nic, msg)
	if resp != nil && resp.Error() != nil {
		this.preCommitMtx.Lock()
		delete(this.preCommit, msg.Tr_Id())
		this.preCommitMtx.Unlock()
		msg.SetTr_State(ifs.Failed)
		msg.SetTr_ErrMsg("T04_Commit.commitInternal: Handle Error: " + msg.Tr_Id() + " " + resp.Error().Error())
		this.nic.Resources().Logger().Debug(msg.Tr_ErrMsg())

		return L8TransactionFor(msg)
	}
	this.nic.Resources().Logger().Debug("T04_Commit.commitInternal: Transaction commited on node ",
		this.nic.Resources().SysConfig().LocalUuid, " - ", msg.Tr_Id())
	msg.SetTr_State(ifs.Committed)
	return L8TransactionFor(msg)
}

func (this *ServiceTransactions) setPreCommitObject(msg *ifs.Message) error {

	pb, err := protocol.ElementsOf(msg, this.nic.Resources())
	if err != nil {
		return err
	}

	if msg.Action() == ifs.PUT ||
		msg.Action() == ifs.DELETE ||
		msg.Action() == ifs.PATCH {
		//Get the object before performing the action so we could rollback
		//if necessary.
		resp := this.nic.Resources().Services().TransactionHandle(pb, ifs.GET, this.nic, msg)
		if resp != nil && resp.Error() != nil {
			return resp.Error()
		}
		this.preCommitMtx.Lock()
		defer this.preCommitMtx.Unlock()
		this.preCommit[msg.Tr_Id()] = resp
	} else {
		this.preCommitMtx.Lock()
		defer this.preCommitMtx.Unlock()
		this.preCommit[msg.Tr_Id()] = pb
	}
	return nil
}
