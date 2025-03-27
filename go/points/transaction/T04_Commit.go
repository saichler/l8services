package transaction

import (
	"github.com/saichler/layer8/go/overlay/protocol"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"google.golang.org/protobuf/proto"
	"time"
)

func (this *ServiceTransactions) commit(msg *types.Message, vnic common.IVirtualNetworkInterface) bool {
	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()

	if msg.Tr.State != types.TransactionState_Commit {
		panic("commit: Unexpected transaction state " + msg.Tr.State.String())
	}

	if this.locked == nil {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Commit: No pending transaction"
		return false
	}

	if this.locked.Tr.Id != msg.Tr.Id {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Commit: commit is for another transaction"
		return false
	}

	if this.locked.Tr.State != types.TransactionState_Locked &&
		this.locked.Tr.State != types.TransactionState_Commit { //The state will be commit if the message hit the leader
		msg.Tr.Error = "Commit: Transaction is not in locked state " + msg.Tr.State.String()
		msg.Tr.State = types.TransactionState_Errored
		return false
	}

	if time.Now().Unix()-this.locked.Tr.StartTime >= 20 { //@TODO add the timeout
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Commit: Transaction has timed out"
		return false
	}

	servicePoints := vnic.Resources().ServicePoints()
	if msg.Action == types.Action_Notify {
		//_, err := servicePoints.Notify()
	} else {
		pb, err := protocol.ProtoOf(this.locked, vnic.Resources())
		if err != nil {
			msg.Tr.State = types.TransactionState_Errored
			msg.Tr.Error = "Commit: Protocol Error: " + err.Error()
			return false
		}
		ok := this.setPreCommitObject(msg, vnic)
		if !ok {
			msg.Tr.State = types.TransactionState_Errored
			msg.Tr.Error = "Commit: Could not set pre-commit object"
			return false
		}
		resp := servicePoints.Handle(pb, this.locked.Action, vnic, this.locked, true)
		if resp.Error() != nil {
			msg.Tr.State = types.TransactionState_Errored
			msg.Tr.Error = "Commit: Handle Error: " + resp.Error().Error()
			return false
		}
		this.locked.Tr.State = types.TransactionState_Commited
	}

	msg.Tr.State = types.TransactionState_Commited
	return true
}

func (this *ServiceTransactions) setPreCommitObject(msg *types.Message, vnic common.IVirtualNetworkInterface) bool {

	pb, err := protocol.ProtoOf(this.locked, vnic.Resources())
	if err != nil {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Pre Commit Object Fetch: Protocol Error: " + err.Error()
		return false
	}

	if msg.Action == types.Action_PUT ||
		msg.Action == types.Action_DELETE ||
		msg.Action == types.Action_PATCH {
		servicePoints := vnic.Resources().ServicePoints()
		//Get the object before performing the action so we could rollback
		//if necessary.
		resp := servicePoints.Handle(pb, types.Action_GET, vnic, this.locked, true)
		if resp.Error() != nil {
			msg.Tr.State = types.TransactionState_Errored
			msg.Tr.Error = "Pre Commit Object Fetch: Service Point: " + resp.Error().Error()
			return false
		}
		this.preCommitObject = resp.List()[0].(proto.Message)
	} else {
		this.preCommitObject = pb
	}
	return true
}
