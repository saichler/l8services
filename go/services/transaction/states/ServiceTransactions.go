package states

import (
	"bytes"
	"strconv"
	"sync"

	"github.com/saichler/l8bus/go/overlay/protocol"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8services"
)

type ServiceTransactions struct {
	mtx            *sync.Mutex
	cond           *sync.Cond
	queue          []*ifs.Message
	running        bool
	nic            ifs.IVNic
	concurrentGets bool

	preCommit    map[string]interface{}
	preCommitMtx *sync.Mutex
}

func newServiceTransactions(concurrentGets bool, nic ifs.IVNic) *ServiceTransactions {
	serviceTransactions := &ServiceTransactions{}
	serviceTransactions.mtx = &sync.Mutex{}
	serviceTransactions.cond = sync.NewCond(serviceTransactions.mtx)
	serviceTransactions.queue = make([]*ifs.Message, 0)
	serviceTransactions.running = true
	serviceTransactions.nic = nic
	serviceTransactions.concurrentGets = concurrentGets
	serviceTransactions.preCommitMtx = &sync.Mutex{}
	serviceTransactions.preCommit = map[string]interface{}{}

	go serviceTransactions.processTransactions()
	return serviceTransactions
}

func (this *ServiceTransactions) shouldHandleAsTransaction(msg *ifs.Message, vnic ifs.IVNic) (ifs.IElements, bool) {
	if msg.Action() == ifs.GET {
		//now := time.Now().UnixMilli()
		this.mtx.Lock()
		defer this.mtx.Unlock()

		/*@TODO
		if this.concurrentGets {
			for this.running {
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
		}*/

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

func (this *ServiceTransactions) addTransaction(msg *ifs.Message, vnic ifs.IVNic) error {
	msg.SetTr_State(ifs.Queued)
	if vnic.Resources().SysConfig().LocalUuid != vnic.Resources().Services().GetLeader(msg.ServiceName(), msg.ServiceArea()) {
		return vnic.Resources().Logger().Error("A non leader has got the message")
	}
	this.mtx.Lock()
	defer this.mtx.Unlock()
	this.queue = append(this.queue, msg)
	this.cond.Broadcast()
	return nil
}

func (this *ServiceTransactions) Next() *ifs.Message {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	for this.running && len(this.queue) == 0 {
		this.cond.Wait()
	}

	if !this.running {
		return nil
	}

	msg := this.queue[0]
	this.queue = this.queue[1:]
	return msg
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

func L8TransactionOf(msg *ifs.Message) *l8services.L8Transaction {
	return &l8services.L8Transaction{State: int32(msg.Tr_State()),
		Id:      msg.Tr_Id(),
		ErrMsg:  msg.Tr_ErrMsg(),
		Created: msg.Tr_Created(),
		Queued:  msg.Tr_Queued(),
		Running: msg.Tr_Running(),
		End:     msg.Tr_End(),
	}
}

func L8TransactionFor(msg *ifs.Message) ifs.IElements {
	return object.New(nil, L8TransactionOf(msg))
}
