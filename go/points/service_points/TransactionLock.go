package service_points

import (
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
	"strings"
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
		tr.state = msg.Tr.State
		this.currentTransactions[msg.Type] = tr
		return msg.Tr
	}

	//We need at least one transaction to pass through the lock
	if strings.Compare(tr.id, msg.Tr.Id) == -1 && tr.state == types.TrState_Locked {
		tr, ok = createTransaction(msg, resourcs)
		if !ok {
			return msg.Tr
		}
		msg.Tr.State = types.TrState_Locked
		tr.state = msg.Tr.State
		this.currentTransactions[msg.Type] = tr
		return msg.Tr
	}

	msg.Tr.State = types.TrState_Errored
	return msg.Tr
}
