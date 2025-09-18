package states

import (
	"bytes"
	"strconv"
	"sync"
	"time"

	"github.com/saichler/l8services/go/services/transaction"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8utils/go/utils/strings"
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/layer8/go/overlay/protocol"
)

type ServiceTransactions struct {
	transactionsMap  *sync.Map
	lockedTrId       string
	mtx              *sync.Mutex
	cond             *sync.Cond
	running          bool
	logger           ifs.ILogger
	transactionQueue []string
	concurrentGets   bool
}

func newServiceTransactions(concurrentGets bool) *ServiceTransactions {
	serviceTransactions := &ServiceTransactions{}
	serviceTransactions.transactionsMap = &sync.Map{}
	serviceTransactions.transactionQueue = make([]string, 0)
	serviceTransactions.mtx = &sync.Mutex{}
	serviceTransactions.cond = sync.NewCond(serviceTransactions.mtx)
	serviceTransactions.running = true
	serviceTransactions.concurrentGets = concurrentGets
	go serviceTransactions.processTransactions()
	return serviceTransactions
}

func (this *ServiceTransactions) shouldHandleAsTransaction(msg *ifs.Message, vnic ifs.IVNic) (ifs.IElements, bool) {
	if msg.Action() == ifs.GET {
		now := time.Now().UnixMilli()
		this.mtx.Lock()
		defer this.mtx.Unlock()

		if this.concurrentGets {
			for this.running && this.lockedTrId != "" {
				t, ok := this.transactionsMap.Load(this.lockedTrId)
				if !ok {
					break
				}
				tr := t.(*transaction.Transaction)
				if tr.Msg().Tr_StartTime() > now {
					break
				}
				this.cond.Wait()
				this.cond.Broadcast()
			}
		}

		if !this.running {
			return nil, false
		}

		pb, err := protocol.ElementsOf(msg, vnic.Resources())
		if err != nil {
			return object.NewError(err.Error()), false
		}

		services := vnic.Resources().Services()
		resp := replicationGet(pb, services, msg, vnic)
		if resp != nil {
			return resp, false
		}
		resp = services.TransactionHandle(pb, msg.Action(), vnic, msg)
		return resp, false
	}
	return nil, true
}

func (this *ServiceTransactions) addTransaction(msg *ifs.Message, vnic ifs.IVNic) *transaction.Transaction {
	msg.SetTr_State(ifs.Create)
	tr := transaction.NewTransaction(msg, vnic)
	this.transactionsMap.Store(msg.Tr_Id(), tr)
	return tr
}

func (this *ServiceTransactions) delTransaction(msg *ifs.Message, st ifs.TransactionState) {
	msg.SetTr_State(st)
	this.transactionsMap.Delete(msg.Tr_Id())
}

func (this *ServiceTransactions) finishInternal(msg *ifs.Message) {
	msg.SetTr_State(ifs.Finished)
	this.transactionsMap.Delete(msg.Tr_Id())
	if this.lockedTrId == msg.Tr_Id() {
		this.lockedTrId = ""
	}
}

func (this *ServiceTransactions) finishFollower(msg *ifs.Message) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	this.finishInternal(msg)
}

func (this *ServiceTransactions) getTransaction(msg *ifs.Message, vnic ifs.IVNic) (*transaction.Transaction, error) {
	t, ok := this.transactionsMap.Load(msg.Tr_Id())
	if !ok {
		//Sleep a second, maybe there is a race condition and the transactin will arrive later
		time.Sleep(time.Second)
		t, ok = this.transactionsMap.Load(msg.Tr_Id())
		if ok {
			return t.(*transaction.Transaction), nil
		}
		hc := health.Health(vnic.Resources())
		from := hc.Health(msg.Source())
		to := hc.Health(vnic.Resources().SysConfig().LocalUuid)
		errMsg := strings.New("ServiceTransactions.getTransaction: Can't find transaction ID ", msg.Tr_Id(), " from ", from.Alias, " to ", to.Alias).String()
		return nil, vnic.Resources().Logger().Error(errMsg)
	}
	tr := t.(*transaction.Transaction)
	tr.SetVnic(vnic)
	return tr, nil
}

func (this *ServiceTransactions) addToQueue(trId string) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	this.transactionQueue = append(this.transactionQueue, trId)
	this.cond.Broadcast()
}

func (this *ServiceTransactions) start(msg *ifs.Message, vnic ifs.IVNic) (ifs.TransactionState, string) {
	vnic.Resources().Logger().Debug("ServiceTransactions.start: ", msg.Tr_Id())
	tr, err := this.getTransaction(msg, vnic)
	if err != nil {
		err = vnic.Resources().Logger().Error("ServiceTransactions.start", "error getting transaction ", msg.Tr_Id(), " ", err.Error())
		return ifs.Errored, err.Error()
	}

	tr.Msg().SetTr_State(msg.Tr_State())

	tr.Cond().L.Lock()
	defer tr.Cond().L.Unlock()

	go this.addToQueue(tr.Msg().Tr_Id())

	vnic.Resources().Logger().Debug("ServiceTransactions.start: Before waiting for transaction to finish ", tr.Msg().Tr_Id())

	tr.Cond().Wait()

	vnic.Resources().Logger().Debug("ServiceTransactions.start: Transaction ended ", tr.Msg().Tr_Id())

	msg.SetTr_State(tr.Msg().Tr_State())
	msg.SetTr_ErrMsg(tr.Msg().Tr_ErrMsg())
	msg.SetTr_StartTime(tr.Msg().Tr_StartTime())
	return tr.Msg().Tr_State(), tr.Msg().Tr_ErrMsg()
}

func (this *ServiceTransactions) Next() *transaction.Transaction {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	for this.running && len(this.transactionQueue) == 0 {
		this.cond.Wait()
	}
	if !this.running {
		return nil
	}

	for this.running && len(this.transactionQueue) > 0 {
		trId := this.transactionQueue[0]
		this.transactionQueue = this.transactionQueue[1:]
		t, ok := this.transactionsMap.Load(trId)
		if !ok {
			this.logger.Error("ServiceTransaction.Next: Cannot find transaction ID ", trId, ", Skipping")
			continue
		}
		return t.(*transaction.Transaction)
	}
	return nil
}

func (this *ServiceTransactions) processTransactions() {
	for this.running {
		tr := this.Next()
		if tr == nil {
			continue
		}
		this.run(tr)
	}
}

func ServiceKey(serviceName string, serviceArea byte) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}

func TransactionOf(tr *transaction.Transaction) *types.Transaction {
	return TransactionFor(tr.Msg())
}

func TransactionFor(msg *ifs.Message) *types.Transaction {
	return &types.Transaction{State: int32(msg.Tr_State()),
		Id:        msg.Tr_Id(),
		ErrMsg:    msg.Tr_ErrMsg(),
		StartTime: msg.Tr_StartTime()}
}
