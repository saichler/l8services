package transaction

import (
	"bytes"
	"github.com/saichler/layer8/go/overlay/protocol"
	"github.com/saichler/serializer/go/serialize/response"
	"github.com/saichler/shared/go/share/maps"
	"github.com/saichler/shared/go/share/queues"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"google.golang.org/protobuf/proto"
	"strconv"
	"sync"
)

type ServiceTransactions struct {
	trMap           *maps.SyncMap
	trVnicMap       *maps.SyncMap
	trCondsMap      *maps.SyncMap
	trQueue         *queues.Queue
	locked          *types.Message
	preCommitObject proto.Message
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

func (this *ServiceTransactions) shouldHandleAsTransaction(msg *types.Message, vnic common.IVirtualNetworkInterface) (common.IResponse, bool) {
	if msg.Action == types.Action_GET {
		this.trCond.L.Lock()
		defer this.trCond.L.Unlock()
		for this.locked != nil {
			this.trCond.Wait()
		}
		servicePoints := vnic.Resources().ServicePoints()
		pb, err := protocol.ProtoOf(msg, vnic.Resources())
		if err != nil {
			return response.NewError(err.Error()), false
		}
		resp := servicePoints.Handle(pb, msg.Action, vnic, msg, true)
		return resp, false
	}
	return nil, true
}

func (this *ServiceTransactions) addTransaction(msg *types.Message) {
	msg.Tr.State = types.TransactionState_Create
	this.trMap.Put(msg.Tr.Id, msg)
}

func (this *ServiceTransactions) delTransaction(msg *types.Message) {
	msg.Tr.State = types.TransactionState_Errored
	this.trMap.Delete(msg.Tr.Id)
}

func (this *ServiceTransactions) finish(msg *types.Message) {
	this.trCond.L.Lock()
	defer func() {
		this.trCond.Broadcast()
		this.trCond.L.Unlock()
	}()

	if this.locked == nil {
		this.preCommitObject = nil
		return
	}

	if this.locked.Tr.Id == msg.Tr.Id {
		this.locked = nil
		this.preCommitObject = nil
	}
	this.trMap.Delete(msg.Tr.Id)
	this.trVnicMap.Delete(msg.Tr.Id)
	msg.Tr.State = types.TransactionState_Finished
}

func (this *ServiceTransactions) start(msg *types.Message, vnic common.IVirtualNetworkInterface) {
	this.trVnicMap.Put(msg.Tr.Id, vnic)
	trCond := sync.NewCond(&sync.Mutex{})
	this.trCondsMap.Put(msg.Tr.Id, trCond)

	m, ok := this.trMap.Get(msg.Tr.Id)
	if !ok {
		panic("error")
	}
	message := m.(*types.Message)
	message.Tr.State = msg.Tr.State

	trCond.L.Lock()
	defer trCond.L.Unlock()
	this.trQueue.Add(msg.Tr.Id)
	trCond.Wait()
	msg.Tr = message.Tr
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
		vnic := v.(common.IVirtualNetworkInterface)
		msg := m.(*types.Message)
		cond := c.(*sync.Cond)
		this.run(msg, vnic, cond)
	}
}

func ServiceKey(serviceName string, serviceArea int32) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}
