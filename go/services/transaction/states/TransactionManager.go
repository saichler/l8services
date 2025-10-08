package states

import (
	"sync"

	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8utils/go/utils/strings"
)

type TransactionManager struct {
	serviceTransactions map[string]*ServiceTransactions
	services            ifs.IServices
	mtx                 *sync.Mutex
}

func NewTransactionManager(services ifs.IServices) *TransactionManager {
	tm := &TransactionManager{}
	tm.mtx = &sync.Mutex{}
	tm.services = services
	tm.serviceTransactions = make(map[string]*ServiceTransactions)
	return tm
}

func (this *TransactionManager) transactionsOf(msg *ifs.Message, nic ifs.IVNic) *ServiceTransactions {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	serviceKey := ServiceKey(msg.ServiceName(), msg.ServiceArea())
	st, ok := this.serviceTransactions[serviceKey]
	if !ok {
		h, ook := this.services.ServiceHandler(msg.ServiceName(), msg.ServiceArea())
		if !ook {
			panic(strings.New("Cannot find service handler for ", msg.ServiceName(), " - ", msg.ServiceArea()).String())
		}
		this.serviceTransactions[serviceKey] = newServiceTransactions(h.TransactionConfig().ConcurrentGets(), nic)
		st = this.serviceTransactions[serviceKey]
	}
	return st
}

func (this *TransactionManager) Run(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	switch msg.Tr_State() {
	case ifs.Created:
		return this.created(msg, vnic)
	case ifs.Running:
		return this.commit(msg, vnic)
	case ifs.Rollback:
		return this.rollback(msg, vnic)
	case ifs.Cleanup:
		return this.cleanup(msg, vnic)
	default:
		panic("Unexpected transaction state " + msg.Tr_State().String() + ":" + msg.Tr_ErrMsg())
	}
}

// First we insert the transaction to the Queue and mark it as queued
func (this *TransactionManager) created(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	st := this.transactionsOf(msg, vnic)
	err := st.addTransaction(msg, vnic)
	if err != nil {
		msg.SetTr_State(ifs.Failed)
		msg.SetTr_ErrMsg(err.Error())
	}
	return L8TransactionFor(msg)
}

func (this *TransactionManager) commit(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	st := this.transactionsOf(msg, vnic)
	return st.commitInternal(msg)
}

func (this *TransactionManager) rollback(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	st := this.transactionsOf(msg, vnic)
	return st.rollbackInternal(msg)
}

func (this *TransactionManager) cleanup(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	st := this.transactionsOf(msg, vnic)
	return st.cleanupInternal(msg)
}
