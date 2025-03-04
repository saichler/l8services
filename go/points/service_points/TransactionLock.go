package service_points

import (
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
)

func (this *Transactions) topicLock(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) bool {
	msg.Tr.State = types.TrState_Lock
	return this.requestFromAllPeers(msg, vnic)
}

func (this *Transactions) localLock(msg *types.Message, resourcs interfaces.IResources) *types.Tr {
	this.cond.L.Lock()
	defer this.cond.L.Unlock()
	tr, ok := this.currentTransactions[msg.Type]
	if !ok {
		tr, ok = createTransaction(msg, resourcs)
		if !ok {
			return msg.Tr
		}
		msg.Tr.State = types.TrState_Locked
		tr.lastState = msg.Tr.State
		this.currentTransactions[msg.Type] = tr
		return msg.Tr
	}
	msg.Tr.State = types.TrState_Errored
	return msg.Tr
}
