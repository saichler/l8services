package states

import (
	"github.com/saichler/l8bus/go/overlay/protocol"
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceTransactions) internalGet(msg *ifs.Message) ifs.IElements {
	pb, err := protocol.ElementsOf(msg, this.nic.Resources())
	if err != nil {
		msg.SetTr_State(ifs.Failed)
		msg.SetTr_ErrMsg("T04_Commit.commitInternal: Protocol Error: " + msg.Tr_Id() + " " + err.Error())
		this.nic.Resources().Logger().Debug(msg.Tr_Id() + " " + err.Error())
		return L8TransactionFor(msg)
	}

	if pb.IsReplica() {
		return this.replicationGet(pb, msg)
	}

	service, _ := this.nic.Resources().Services().ServiceHandler(msg.ServiceName(), msg.ServiceArea())
	if service.TransactionConfig().Replication() {
		return this.replicationGet(pb, msg)
	}

	return this.nic.Resources().Services().TransactionHandle(pb, msg.Action(), this.nic, msg)
}

func (this *ServiceTransactions) replicationGet(pb ifs.IElements, msg *ifs.Message) ifs.IElements {
	return this.nic.Resources().Services().TransactionHandle(pb, msg.Action(), this.nic, msg)
}
