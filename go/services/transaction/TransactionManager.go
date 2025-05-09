package transaction

import (
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
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

func (this *TransactionManager) transactionsOf(msg ifs.IMessage) *ServiceTransactions {
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

func (this *TransactionManager) Run(msg ifs.IMessage, vnic ifs.IVNic) ifs.IElements {
	switch msg.Tr().State() {
	case ifs.Create:
		this.create(msg, vnic.Resources().Logger())
	case ifs.Start:
		this.start(msg, vnic)
	case ifs.Lock:
		this.lock(msg, vnic.Resources().Logger())
	case ifs.Commit:
		this.commit(msg, vnic)
	case ifs.Finish:
		this.finish(msg, vnic.Resources().Logger())
	case ifs.Rollback:
		this.rollback(msg, vnic)
	case ifs.Errored:
	default:
		panic("Unexpected transaction state " + msg.Tr().State().String() + ":" + msg.Tr().ErrorMessage())
	}
	return object.New(nil, msg.Tr())
}

func (this *TransactionManager) create(msg ifs.IMessage, log ifs.ILogger) {
	if msg.Tr().State() != ifs.Create {
		panic("create: Unexpected transaction state " + msg.Tr().State().String())
	}
	createTransaction(msg)
	log.Info("Tr ", msg.Tr().Id(), " created!")
	st := this.transactionsOf(msg)
	st.addTransaction(msg)
	msg.Tr().SetState(ifs.Created)
}

func (this *TransactionManager) lock(msg ifs.IMessage, log ifs.ILogger) {
	log.Debug("Tr Lock...")
	st := this.transactionsOf(msg)
	st.lock(msg)
}

func (this *TransactionManager) commit(msg ifs.IMessage, vnic ifs.IVNic) {
	vnic.Resources().Logger().Debug("Tr Commit...")
	st := this.transactionsOf(msg)
	st.commit(msg, vnic)
}

func (this *TransactionManager) rollback(msg ifs.IMessage, vnic ifs.IVNic) {
	vnic.Resources().Logger().Debug("Tr Create...")
	st := this.transactionsOf(msg)
	st.rollback(msg, vnic)
}

func (this *TransactionManager) finish(msg ifs.IMessage, log ifs.ILogger) {
	log.Debug("Tr Finish...")
	st := this.transactionsOf(msg)
	st.finish(msg)
}

func (this *TransactionManager) start(msg ifs.IMessage, vnic ifs.IVNic) {
	vnic.Resources().Logger().Debug("Tr Start...")
	st := this.transactionsOf(msg)
	st.start(msg, vnic)
}
