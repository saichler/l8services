package states

import (
	"bytes"
	"strconv"
	"sync"

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
