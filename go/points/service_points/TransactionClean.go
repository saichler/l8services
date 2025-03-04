package service_points

import (
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
)

func (this *Transactions) topicClean(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) bool {
	msg.Tr.State = types.TrState_Clean
	return this.requestFromAllPeers(msg, vnic)
}

func (this *Transactions) localClean(msg *types.Message) *types.Tr {
	this.cond.L.Lock()
	defer this.cond.L.Unlock()
	msg.Tr.State = types.TrState_Cleaned
	tr, ok := this.currentTransactions[msg.Type]
	if ok && tr.id == msg.Tr.Id {
		delete(this.currentTransactions, msg.Type)
	}
	return msg.Tr
}
