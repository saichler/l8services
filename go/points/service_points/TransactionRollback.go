package service_points

import (
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
)

func (this *Transactions) topicRollback(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) bool {
	msg.Tr.State = types.TrState_Rollback
	return this.requestFromAllPeers(msg, vnic)
}

func (this *Transactions) localRollback(msg *types.Message) *types.Tr {
	tr, ok := this.currentTransactions[msg.Type]
	if !ok {
		msg.Tr.State = types.TrState_Errored
		msg.Tr.Error = "Rollback: No commited transaction"
		return msg.Tr
	}

	if tr.id != msg.Tr.Id {
		msg.Tr.State = types.TrState_Errored
		msg.Tr.Error = "Rollback: commited is for another transaction"
		return msg.Tr
	}

	if tr.lastState != types.TrState_Commited {
		msg.Tr.State = types.TrState_Errored
		msg.Tr.Error = "Rollback: Transaction state is not in commited state"
		return msg.Tr
	}

	//@TODO - Do rollback logic here
	//Rollback logic here

	msg.Tr.State = types.TrState_Rollbacked
	return msg.Tr
}
