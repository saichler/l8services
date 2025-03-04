package service_points

import (
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
	"time"
)

func (this *Transactions) topicCommit(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) bool {
	msg.Tr.State = types.TrState_Commit
	return this.requestFromAllPeers(msg, vnic)
}

func (this *Transactions) localCommit(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) *types.Tr {
	tr, ok := this.currentTransactions[msg.Type]
	if !ok {
		msg.Tr.State = types.TrState_Errored
		msg.Tr.Error = "Commit: No pending transaction"
		return msg.Tr
	}

	if tr.id != msg.Tr.Id {
		msg.Tr.State = types.TrState_Errored
		msg.Tr.Error = "Commit: commit is for another transaction"
		return msg.Tr
	}

	if tr.lastState != types.TrState_Locked {
		msg.Tr.State = types.TrState_Errored
		msg.Tr.Error = "Commit: Transaction is not in locked state"
		return msg.Tr
	}

	if time.Now().Unix()-tr.startTime >= 2 { //@TODO add the timeout
		msg.Tr.State = types.TrState_Errored
		msg.Tr.Error = "Commit: Transaction has timed out"
		return msg.Tr
	}

	isLeader, _ := IsLeader(vnic.Resources(), vnic.Resources().Config().LocalUuid, msg.Type, msg.Vlan)
	if isLeader {
		//send commit request to all peers
		ok = this.topicCommit(msg, vnic)
		if !ok {
			msg.Tr.State = types.TrState_Errored
			msg.Tr.Error = "Commit: Transaction failed to commit"
			return msg.Tr
		}
	}

	impl := vnic.Resources().ServicePoints().(*ServicePointsImpl)
	_, err := impl.doAction(tr.handler, tr.action, tr.pb, vnic.Resources())

	if err != nil {
		this.localRollback(msg)
		this.localClean(msg)
		msg.Tr.State = types.TrState_Errored
		msg.Tr.Error = "Commit: " + err.Error()
		return msg.Tr
	}

	this.localClean(msg)

	msg.Tr.State = types.TrState_Commited
	return msg.Tr
}
