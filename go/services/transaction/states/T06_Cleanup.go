package states

import (
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceTransactions) cleanupInternal(msg *ifs.Message) {
	if msg.Action() == ifs.Notify {
		//_, err := services.Notify()
	}

	this.preCommitMtx.Lock()
	defer this.preCommitMtx.Unlock()
	delete(this.preCommit, msg.Tr_Id())
	this.nic.Reply(msg, L8TransactionFor(msg))
}
