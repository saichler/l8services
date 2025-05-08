package transaction

import (
	"bytes"
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/layer8/go/overlay/protocol"
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/l8utils/go/utils/maps"
	"github.com/saichler/l8utils/go/utils/queues"
	"github.com/saichler/l8types/go/ifs"
	"strconv"
	"sync"
	"time"
)

type ServiceTransactions struct {
	trMap           *maps.SyncMap
	trVnicMap       *maps.SyncMap
	trCondsMap      *maps.SyncMap
	trQueue         *queues.Queue
	locked          ifs.IMessage
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

func (this *ServiceTransactions) shouldHandleAsTransaction(msg ifs.IMessage, vnic ifs.IVirtualNetworkInterface) (ifs.IElements, bool) {
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

		servicePoints := vnic.Resources().Services()
		resp := replicationGet(pb, servicePoints, msg, vnic)
		if resp != nil {
			return resp, false
		}
		resp = servicePoints.TransactionHandle(pb, msg.Action(), vnic, msg)
		return resp, false
	}
	return nil, true
}

func (this *ServiceTransactions) addTransaction(msg ifs.IMessage) {
	msg.Tr().SetState(ifs.Create)
	this.trMap.Put(msg.Tr().Id(), msg)
}

func (this *ServiceTransactions) delTransaction(msg ifs.IMessage) {
	msg.Tr().SetState(ifs.Errored)
	this.trMap.Delete(msg.Tr().Id())
}

func (this *ServiceTransactions) finish(msg ifs.IMessage) {
	this.trCond.L.Lock()
	defer func() {
		this.trCond.Broadcast()
		this.trCond.L.Unlock()
	}()

	if this.locked == nil {
		this.preCommitObject = nil
		return
	}

	if this.locked.Tr().Id() == msg.Tr().Id() {
		this.locked = nil
		this.preCommitObject = nil
	}
	this.trMap.Delete(msg.Tr().Id())
	this.trVnicMap.Delete(msg.Tr().Id())
	msg.Tr().SetState(ifs.Finished)
}

func (this *ServiceTransactions) start(msg ifs.IMessage, vnic ifs.IVirtualNetworkInterface) {
	m, ok := this.trMap.Get(msg.Tr().Id())
	if !ok {
		time.Sleep(time.Second)
		m, ok = this.trMap.Get(msg.Tr().Id())
		hc := health.Health(vnic.Resources())
		from := hc.HealthPoint(msg.Source())
		to := hc.HealthPoint(vnic.Resources().SysConfig().LocalUuid)
		okStr := "NO"
		if ok {
			okStr = "YES"
		}
		panic("Can't find transaction ID: " + msg.Tr().Id() + " from " +
			from.Alias + " to " + to.Alias + " ok " + okStr)
	}

	this.trVnicMap.Put(msg.Tr().Id(), vnic)
	trCond := sync.NewCond(&sync.Mutex{})
	this.trCondsMap.Put(msg.Tr().Id(), trCond)

	message := m.(ifs.IMessage)
	message.Tr().SetState(msg.Tr().State())

	trCond.L.Lock()
	defer trCond.L.Unlock()
	this.trQueue.Add(msg.Tr().Id())
	vnic.Resources().Logger().Debug("Before waiting for transaction to finish")
	trCond.Wait()
	vnic.Resources().Logger().Debug("Transaction ended")
	msg.SetTr(message.Tr())
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
		vnic := v.(ifs.IVirtualNetworkInterface)
		msg := m.(ifs.IMessage)
		cond := c.(*sync.Cond)
		this.run(msg, vnic, cond)
	}
}

func ServiceKey(serviceName string, serviceArea uint16) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}
