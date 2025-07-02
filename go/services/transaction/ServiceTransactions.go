package transaction

import (
	"bytes"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types"
	"github.com/saichler/l8utils/go/utils/maps"
	"github.com/saichler/l8utils/go/utils/queues"
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/layer8/go/overlay/protocol"
	"strconv"
	"sync"
	"time"
)

type ServiceTransactions struct {
	trMap           *maps.SyncMap
	trVnicMap       *maps.SyncMap
	trCondsMap      *maps.SyncMap
	trQueue         *queues.Queue
	locked          *ifs.Message
	preCommitObject ifs.IElements
	trCond          *sync.Cond
}

func newServiceTransactions(serviceName string) *ServiceTransactions {
	serviceTransactions := &ServiceTransactions{}
	serviceTransactions.trMap = maps.NewSyncMap()
	serviceTransactions.trVnicMap = maps.NewSyncMap()
	serviceTransactions.trCondsMap = maps.NewSyncMap()
	serviceTransactions.trQueue = queues.NewQueue(serviceName, 5000)
	serviceTransactions.trCond = sync.NewCond(&sync.Mutex{})
	go serviceTransactions.processTransactions()
	return serviceTransactions
}

func (this *ServiceTransactions) shouldHandleAsTransaction(msg *ifs.Message, vnic ifs.IVNic) (ifs.IElements, bool) {
	if msg.Action() == ifs.GET {
		this.trCond.L.Lock()
		defer this.trCond.L.Unlock()
		for this.locked != nil {
			this.trCond.Wait()
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

func (this *ServiceTransactions) addTransaction(msg *ifs.Message) {
	msg.SetTr_State(ifs.Create)
	this.trMap.Put(msg.Tr_Id(), msg)
}

func (this *ServiceTransactions) delTransaction(msg *ifs.Message) {
	msg.SetTr_State(ifs.Errored)
	this.trMap.Delete(msg.Tr_Id())
}

func (this *ServiceTransactions) finish(msg *ifs.Message) {
	this.trCond.L.Lock()
	defer func() {
		this.trCond.Broadcast()
		this.trCond.L.Unlock()
	}()

	if this.locked == nil {
		this.preCommitObject = nil
		return
	}

	if this.locked.Tr_Id() == msg.Tr_Id() {
		this.locked = nil
		this.preCommitObject = nil
	}
	this.trMap.Delete(msg.Tr_Id())
	this.trVnicMap.Delete(msg.Tr_Id())
	msg.SetTr_State(ifs.Finished)
}

func (this *ServiceTransactions) start(msg *ifs.Message, vnic ifs.IVNic) {
	m, ok := this.trMap.Get(msg.Tr_Id())
	if !ok {
		time.Sleep(time.Second)
		m, ok = this.trMap.Get(msg.Tr_Id())
		hc := health.Health(vnic.Resources())
		from := hc.Health(msg.Source())
		to := hc.Health(vnic.Resources().SysConfig().LocalUuid)
		okStr := "NO"
		if ok {
			okStr = "YES"
		}
		panic("Can't find transaction ID: " + msg.Tr_Id() + " from " +
			from.Alias + " to " + to.Alias + " ok " + okStr)
	}

	this.trVnicMap.Put(msg.Tr_Id(), vnic)
	trCond := sync.NewCond(&sync.Mutex{})
	this.trCondsMap.Put(msg.Tr_Id(), trCond)

	message := m.(*ifs.Message)
	message.SetTr_State(msg.Tr_State())

	trCond.L.Lock()
	defer trCond.L.Unlock()
	this.trQueue.Add(msg.Tr_Id())
	vnic.Resources().Logger().Debug("Before waiting for transaction to finish")
	trCond.Wait()
	vnic.Resources().Logger().Debug("Transaction ended")
	msg.SetTr_State(message.Tr_State())
	msg.SetTr_ErrMsg(message.Tr_ErrMsg())
	msg.SetTr_StartTime(message.Tr_StartTime())
}

func (this *ServiceTransactions) processTransactions() {
	for {
		trId := this.trQueue.Next().(string)
		v, ok := this.trVnicMap.Get(trId)
		if !ok {
			panic("Cannot find vnic for tr " + trId)
		}
		m, ok := this.trMap.Get(trId)
		if !ok {
			panic("Cannot find msg for tr " + trId)
		}
		c, ok := this.trCondsMap.Get(trId)
		if !ok {
			panic("Cannot find cond for tr " + trId)
		}
		vnic := v.(ifs.IVNic)
		msg := m.(*ifs.Message)
		cond := c.(*sync.Cond)
		this.run(msg, vnic, cond)
	}
}

func ServiceKey(serviceName string, serviceArea byte) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}

func TransactionOf(msg *ifs.Message) *types.Transaction {
	return &types.Transaction{State: int32(msg.Tr_State()),
		Id:        msg.Tr_Id(),
		ErrMsg:    msg.Tr_ErrMsg(),
		StartTime: msg.Tr_StartTime()}
}
