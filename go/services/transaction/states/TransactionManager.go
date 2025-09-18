package states

import (
	"sync"

	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8services"
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

func (this *TransactionManager) transactionsOf(msg *ifs.Message) *ServiceTransactions {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	serviceKey := ServiceKey(msg.ServiceName(), msg.ServiceArea())
	st, ok := this.serviceTransactions[serviceKey]
	if !ok {
		h, ook := this.services.ServiceHandler(msg.ServiceName(), msg.ServiceArea())
		if !ook {
			panic(strings.New("Cannot find service handler for ", msg.ServiceName(), " - ", msg.ServiceArea()).String())
		}
		this.serviceTransactions[serviceKey] = newServiceTransactions(h.TransactionConfig().ConcurrentGets())
		st = this.serviceTransactions[serviceKey]
	}
	return st
}

func (this *TransactionManager) Run(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	switch msg.Tr_State() {
	case ifs.Create:
		this.create(msg, vnic)
	case ifs.Start:
		this.start(msg, vnic)
	case ifs.Created:
		this.lock(msg, vnic)
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
	return object.New(nil, &l8services.L8Transaction{State: int32(msg.Tr_State()),
		Id:        msg.Tr_Id(),
		ErrMsg:    msg.Tr_ErrMsg(),
		StartTime: msg.Tr_StartTime()})
}

func (this *TransactionManager) create(msg *ifs.Message, vnic ifs.IVNic) {
	if msg.Tr_State() != ifs.Create {
		panic("create: Unexpected transaction state " + msg.Tr_State().String())
	}
	createTransaction(msg)
	st := this.transactionsOf(msg)
	tr := st.addTransaction(msg, vnic)
	tr.Debug("TransactionManager.create: Created Transaction ", tr.Msg().Tr_Id())
	msg.SetTr_State(ifs.Created)
}

func (this *TransactionManager) lock(msg *ifs.Message, vnic ifs.IVNic) {
	vnic.Resources().Logger().Debug("Tr Lock...")
	st := this.transactionsOf(msg)
	state, err := st.lockFollower(msg, vnic)
	msg.SetTr_State(state)
	msg.SetTr_ErrMsg(err)
}

func (this *TransactionManager) commit(msg *ifs.Message, vnic ifs.IVNic) {
	vnic.Resources().Logger().Debug("Tr Commit...")
	st := this.transactionsOf(msg)
	state, err := st.commitFollower(msg, vnic)
	msg.SetTr_State(state)
	msg.SetTr_ErrMsg(err)
}

func (this *TransactionManager) rollback(msg *ifs.Message, vnic ifs.IVNic) {
	vnic.Resources().Logger().Debug("Tr Create...")
	st := this.transactionsOf(msg)
	state, err := st.rollbackFollower(msg, vnic)
	msg.SetTr_State(state)
	msg.SetTr_ErrMsg(err)
}

func (this *TransactionManager) finish(msg *ifs.Message, log ifs.ILogger) {
	log.Debug("TransactionManager.finish: ", msg.Tr_Id())
	st := this.transactionsOf(msg)
	st.finishFollower(msg)
}

func (this *TransactionManager) start(msg *ifs.Message, vnic ifs.IVNic) {
	vnic.Resources().Logger().Debug("TransactionManager.start: ", msg.Tr_Id())
	st := this.transactionsOf(msg)
	st.start(msg, vnic)

}
