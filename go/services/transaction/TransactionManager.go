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

func (this *TransactionManager) transactionsOf(msg *ifs.Message) *ServiceTransactions {
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

func (this *TransactionManager) Run(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	switch msg.Tr_State() {
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
		panic("Unexpected transaction state " + msg.Tr_State().String() + ":" + msg.Tr_ErrMsg())
	}
	return object.New(nil, TransactionOf(msg))
}

func (this *TransactionManager) create(msg *ifs.Message, log ifs.ILogger) {
	if msg.Tr_State() != ifs.Create {
		panic("create: Unexpected transaction state " + msg.Tr_State().String())
	}
	createTransaction(msg)
	log.Info("Tr ", msg.Tr_Id(), " created!")
	st := this.transactionsOf(msg)
	st.addTransaction(msg)
	msg.SetTr_State(ifs.Created)
}

func (this *TransactionManager) lock(msg *ifs.Message, log ifs.ILogger) {
	log.Debug("Tr Lock...")
	st := this.transactionsOf(msg)
	st.lock(msg)
}

func (this *TransactionManager) commit(msg *ifs.Message, vnic ifs.IVNic) {
	vnic.Resources().Logger().Debug("Tr Commit...")
	st := this.transactionsOf(msg)
	st.commit(msg, vnic)
}

func (this *TransactionManager) rollback(msg *ifs.Message, vnic ifs.IVNic) {
	vnic.Resources().Logger().Debug("Tr Create...")
	st := this.transactionsOf(msg)
	st.rollback(msg, vnic)
}

func (this *TransactionManager) finish(msg *ifs.Message, log ifs.ILogger) {
	log.Debug("Tr Finish...")
	st := this.transactionsOf(msg)
	st.finish(msg)
}

func (this *TransactionManager) start(msg *ifs.Message, vnic ifs.IVNic) {
	vnic.Resources().Logger().Debug("Tr Start...")
	st := this.transactionsOf(msg)
	st.start(msg, vnic)
}
