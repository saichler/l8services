package transaction

import (
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/types/go/common"
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

func (this *TransactionManager) transactionsOf(msg common.IMessage) *ServiceTransactions {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	serviceKey := ServiceKey(msg.ServiceName(), msg.ServiceArea())
	st, ok := this.serviceTransactions[serviceKey]
	if !ok {
		this.serviceTransactions[serviceKey] = newServiceTransactions(serviceKey)
		st = this.serviceTransactions[serviceKey]
	}
	return st
}

func (this *TransactionManager) Run(msg common.IMessage, vnic common.IVirtualNetworkInterface) common.IElements {
	switch msg.Tr().State() {
	case common.Create:
		this.create(msg, vnic.Resources().Logger())
	case common.Start:
		this.start(msg, vnic)
	case common.Lock:
		this.lock(msg, vnic.Resources().Logger())
	case common.Commit:
		this.commit(msg, vnic)
	case common.Finish:
		this.finish(msg, vnic.Resources().Logger())
	case common.Rollback:
		this.rollback(msg, vnic)
	case common.Errored:
	default:
		panic("Unexpected transaction state " + msg.Tr().State().String() + ":" + msg.Tr().ErrorMessage())
	}
	return object.New(nil, msg.Tr())
}

func (this *TransactionManager) create(msg common.IMessage, log common.ILogger) {
	log.Debug("Tr Create...")
	if msg.Tr().State() != common.Create {
		panic("create: Unexpected transaction state " + msg.Tr().State().String())
	}
	createTransaction(msg)
	st := this.transactionsOf(msg)
	st.addTransaction(msg)
	msg.Tr().SetState(common.Created)
}

func (this *TransactionManager) lock(msg common.IMessage, log common.ILogger) {
	log.Debug("Tr Lock...")
	st := this.transactionsOf(msg)
	st.lock(msg)
}

func (this *TransactionManager) commit(msg common.IMessage, vnic common.IVirtualNetworkInterface) {
	vnic.Resources().Logger().Debug("Tr Commit...")
	st := this.transactionsOf(msg)
	st.commit(msg, vnic)
}

func (this *TransactionManager) rollback(msg common.IMessage, vnic common.IVirtualNetworkInterface) {
	vnic.Resources().Logger().Debug("Tr Create...")
	st := this.transactionsOf(msg)
	st.rollback(msg, vnic)
}

func (this *TransactionManager) finish(msg common.IMessage, log common.ILogger) {
	log.Debug("Tr Finish...")
	st := this.transactionsOf(msg)
	st.finish(msg)
}

func (this *TransactionManager) start(msg common.IMessage, vnic common.IVirtualNetworkInterface) {
	vnic.Resources().Logger().Debug("Tr Start...")
	st := this.transactionsOf(msg)
	st.start(msg, vnic)
}
