package states

import (
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceTransactions) cleanupInternal(msg *ifs.Message) ifs.IElements {
	if msg.Action() == ifs.Notify {
		return nil
	}
	this.preCommitMtx.Lock()
	defer this.preCommitMtx.Unlock()
	delete(this.preCommit, msg.Tr_Id())
	return L8TransactionFor(msg)
}
