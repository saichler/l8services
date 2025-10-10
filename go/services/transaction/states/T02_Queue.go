package states

import (
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
		return this.internalGet(msg)
	}

	err := this.addTransaction(msg, vnic)
	if err != nil {
		msg.SetTr_State(ifs.Failed)
		msg.SetTr_ErrMsg(err.Error())
	}
	return L8TransactionFor(msg)
}
