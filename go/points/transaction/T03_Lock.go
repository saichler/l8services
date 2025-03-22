package transaction

import (
	"github.com/saichler/types/go/types"
	"strconv"
)

func (this *ServiceTransactions) lock(msg *types.Message) bool {
	if msg.Tr.State != types.TransactionState_Lock {
		panic("lock: Unexpected transaction state " + msg.Tr.State.String())
	}

	m, ok := this.trMap.Get(msg.Tr.Id)
	if !ok {
		msg.Tr.State = types.TransactionState_LockFailed
		msg.Tr.Error = "Did not find transaction in pending transactions"
		return false
	}
	message := m.(*types.Message)

	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()

	if this.locked == nil {
		this.locked = message
		msg.Tr.State = types.TransactionState_Locked
		message.Tr.State = msg.Tr.State
		return true
	}

	msg.Tr.State = types.TransactionState_LockFailed
	msg.Tr.Error = "Failed to lock : " + msg.ServiceName + ":" + strconv.Itoa(int(msg.ServiceArea))
	return false
}
