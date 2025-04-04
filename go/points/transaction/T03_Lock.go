package transaction

import (
	"github.com/saichler/types/go/common"
	"strconv"
)

func (this *ServiceTransactions) lock(msg common.IMessage) bool {
	if msg.Tr().State() != common.Lock {
		panic("lock: Unexpected transaction state " + msg.Tr().State().String())
	}

	m, ok := this.trMap.Get(msg.Tr().Id())
	if !ok {
		msg.Tr().SetState(common.LockFailed)
		msg.Tr().SetErrorMessage("Did not find transaction in pending transactions")
		return false
	}
	message := m.(common.IMessage)

	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()

	if this.locked == nil {
		this.locked = message
		msg.Tr().SetState(common.Locked)
		message.Tr().SetState(msg.Tr().State())
		return true
	}

	msg.Tr().SetState(common.LockFailed)
	msg.Tr().SetErrorMessage("Failed to lock : " + msg.ServiceName() + ":" + strconv.Itoa(int(msg.ServiceArea())))
	return false
}
