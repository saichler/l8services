package states

import (
	"github.com/saichler/l8bus/go/overlay/protocol"
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceTransactions) addTransaction(msg *ifs.Message, vnic ifs.IVNic) error {
	msg.SetTr_State(ifs.Queued)
	if vnic.Resources().SysConfig().LocalUuid != vnic.Resources().Services().GetLeader(msg.ServiceName(), msg.ServiceArea()) {
		return vnic.Resources().Logger().Error("A non leader has got the message")
	}
	this.mtx.Lock()
	defer this.mtx.Unlock()
	this.queue = append(this.queue, msg)
	this.cond.Broadcast()
	return nil
}

func (this *ServiceTransactions) queueTransaction(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	if msg.Action() == ifs.GET {
		pb, err := protocol.ElementsOf(msg, this.nic.Resources())
		if err != nil {
			msg.SetTr_State(ifs.Failed)
			msg.SetTr_ErrMsg("T04_Commit.commitInternal: Protocol Error: " + msg.Tr_Id() + " " + err.Error())
			this.nic.Resources().Logger().Debug(msg.Tr_Id() + " " + err.Error())
			return L8TransactionFor(msg)
		}
		return this.nic.Resources().Services().TransactionHandle(pb, msg.Action(), this.nic, msg)
	}

	err := this.addTransaction(msg, vnic)
	if err != nil {
		msg.SetTr_State(ifs.Failed)
		msg.SetTr_ErrMsg(err.Error())
	}
	return L8TransactionFor(msg)
}
