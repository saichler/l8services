package transaction

import (
	"github.com/saichler/l8types/go/ifs"
	"strconv"
)

func (this *ServiceTransactions) lock(msg *ifs.Message) bool {
	if msg.Tr_State() != ifs.Lock {
		panic("lock: Unexpected transaction state " + msg.Tr_State().String())
	}

	m, ok := this.trMap.Get(msg.Tr_Id())
	if !ok {
		msg.SetTr_State(ifs.LockFailed)
		msg.SetTr_ErrMsg("Did not find transaction in pending transactions")
		return false
	}
	message := m.(*ifs.Message)

	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()

	if this.locked == nil {
		this.locked = message
		msg.SetTr_State(ifs.Locked)
		message.SetTr_State(msg.Tr_State())
		return true
	}

	msg.SetTr_State(ifs.LockFailed)
	msg.SetTr_ErrMsg("Failed to lock : " + msg.ServiceName() + ":" + strconv.Itoa(int(msg.ServiceArea())))
	return false
}
