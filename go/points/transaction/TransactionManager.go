package transaction

import (
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"sync"
)

type TransactionManager struct {
	serviceTransactions map[string]*ServiceTransactions
	mtx                 *sync.Mutex
}

func NewTransactionManager() *TransactionManager {
	tm := &TransactionManager{}
	tm.mtx = &sync.Mutex{}
	tm.serviceTransactions = make(map[string]*ServiceTransactions)
	return tm
}

func (this *TransactionManager) transactionsOf(msg *types.Message) *ServiceTransactions {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	serviceKey := ServiceKey(msg.ServiceName, msg.ServiceArea)
	st, ok := this.serviceTransactions[serviceKey]
	if !ok {
		this.serviceTransactions[serviceKey] = newServiceTransactions(serviceKey)
		st = this.serviceTransactions[serviceKey]
	}
	return st
}

func (this *TransactionManager) Run(msg *types.Message, vnic common.IVirtualNetworkInterface) common.IElements {
	switch msg.Tr.State {
	case types.TransactionState_Create:
		this.create(msg)
	case types.TransactionState_Start:
		this.start(msg, vnic)
	case types.TransactionState_Lock:
		this.lock(msg)
	case types.TransactionState_Commit:
		this.commit(msg, vnic)
	case types.TransactionState_Finish:
		this.finish(msg)
	case types.TransactionState_Rollback:
		this.rollback(msg, vnic)
	case types.TransactionState_Errored:
	default:
		panic("Unexpected transaction state " + msg.Tr.State.String() + ":" + msg.Tr.Error)
	}
	return object.New(nil, msg.Tr)
}

func (this *TransactionManager) create(msg *types.Message) {
	if msg.Tr.State != types.TransactionState_Create {
		panic("create: Unexpected transaction state " + msg.Tr.State.String())
	}
	createTransaction(msg)
	st := this.transactionsOf(msg)
	st.addTransaction(msg)
	msg.Tr.State = types.TransactionState_Created
}

func (this *TransactionManager) lock(msg *types.Message) {
	st := this.transactionsOf(msg)
	st.lock(msg)
}

func (this *TransactionManager) commit(msg *types.Message, vnic common.IVirtualNetworkInterface) {
	st := this.transactionsOf(msg)
	st.commit(msg, vnic)
}

func (this *TransactionManager) rollback(msg *types.Message, vnic common.IVirtualNetworkInterface) {
	st := this.transactionsOf(msg)
	st.rollback(msg, vnic)
}

func (this *TransactionManager) finish(msg *types.Message) {
	st := this.transactionsOf(msg)
	st.finish(msg)
}

func (this *TransactionManager) start(msg *types.Message, vnic common.IVirtualNetworkInterface) {
	st := this.transactionsOf(msg)
	st.start(msg, vnic)
}
