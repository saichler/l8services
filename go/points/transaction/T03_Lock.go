package transaction

import (
	"github.com/saichler/l8types/go/ifs"
	"strconv"
)

func (this *ServiceTransactions) lock(msg ifs.IMessage) bool {
	if msg.Tr().State() != ifs.Lock {
		panic("lock: Unexpected transaction state " + msg.Tr().State().String())
	}

	m, ok := this.trMap.Get(msg.Tr().Id())
	if !ok {
		msg.Tr().SetState(ifs.LockFailed)
		msg.Tr().SetErrorMessage("Did not find transaction in pending transactions")
		return false
	}
	message := m.(ifs.IMessage)

	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()

	if this.locked == nil {
		this.locked = message
		msg.Tr().SetState(ifs.Locked)
		message.Tr().SetState(msg.Tr().State())
		return true
	}

	msg.Tr().SetState(ifs.LockFailed)
	msg.Tr().SetErrorMessage("Failed to lock : " + msg.ServiceName() + ":" + strconv.Itoa(int(msg.ServiceArea())))
	return false
}
