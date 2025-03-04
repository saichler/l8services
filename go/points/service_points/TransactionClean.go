package service_points

import (
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
)

func (this *Transactions) topicClean(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) bool {
	msg.Tr.State = types.TrState_Clean
	return this.requestFromAllPeers(msg, vnic)
}

func (this *Transactions) localClean(msg *types.Message) (*types.Tr, *Transaction) {
	this.cond.L.Lock()
	defer this.cond.L.Unlock()
	msg.Tr.State = types.TrState_Cleaned
	tr, ok := this.currentTransactions[msg.Type]
	if ok && tr.id == msg.Tr.Id {
		delete(this.currentTransactions, msg.Type)
		_, ok = this.pendingTransactions[msg.Type]
		if ok && len(this.pendingTransactions[msg.Type]) > 0 {
			nextTr := this.pendingTransactions[msg.Type][0]
			this.pendingTransactions[msg.Type] = this.pendingTransactions[msg.Type][1:]
			nextTr.state = types.TrState_Locked
			this.currentTransactions[msg.Type] = nextTr
			return msg.Tr, nextTr
		}
	}
	return msg.Tr, nil
}

func notifyNextTR(tr *Transaction) {
	if tr == nil {
		return
	}
	tr.cond.L.Lock()
	defer tr.cond.L.Unlock()
	tr.cond.Broadcast()
}
