package service_points

import (
	"github.com/saichler/layer8/go/overlay/protocol"
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
	"sync"
	"time"
)

func createTransaction(msg *types.Message, resourcs interfaces.IResources) (*Transaction, bool) {

	tr := &Transaction{}
	tr.cond = sync.NewCond(&sync.Mutex{})

	if msg.Tr == nil {
		tr.id = interfaces.NewUuid()
		msg.Tr = &types.Tr{}
		msg.Tr.Id = tr.id
		msg.Tr.StartTime = time.Now().Unix()
		tr.startTime = msg.Tr.StartTime
	} else {
		tr.id = msg.Tr.Id
		tr.state = msg.Tr.State
		tr.startTime = msg.Tr.StartTime
	}

	tr.action = msg.Action

	handler, ok := resourcs.ServicePoints().ServicePointHandler(msg.Type)
	if !ok {
		msg.Tr.State = types.TrState_Errored
		msg.Tr.Error = "Tr Create: No service point handler found for " + msg.Type
		return tr, false
	}
	tr.handler = handler

	pb, err := protocol.ProtoOf(msg, resourcs)
	if err != nil {
		msg.Tr.State = types.TrState_Errored
		msg.Tr.Error = "Tr Create: " + err.Error()
		return tr, false
	}

	tr.pb = pb
	return tr, true
}

func (this *Transactions) createTransaction(msg *types.Message, resourcs interfaces.IResources) (bool, *Transaction) {
	this.cond.L.Lock()
	defer this.cond.L.Unlock()

	tr, ok := createTransaction(msg, resourcs)
	if !ok {
		return false, nil
	}

	_, ok = this.currentTransactions[msg.Type]
	if !ok {
		msg.Tr.State = types.TrState_Locked
		tr.state = msg.Tr.State
		this.currentTransactions[msg.Type] = tr
		return true, tr
	}

	msg.Tr.State = types.TrState_Pending
	tr.state = msg.Tr.State
	_, ok = this.pendingTransactions[msg.Type]
	if !ok {
		this.pendingTransactions[msg.Type] = make([]*Transaction, 0)
	}
	this.pendingTransactions[msg.Type] = append(this.pendingTransactions[msg.Type], tr)

	return true, tr
}

func (this *Transactions) checkCurrent(msg *types.Message) bool {
	this.cond.L.Lock()
	defer this.cond.L.Unlock()
	if this.currentTransactions[msg.Type] == nil {
		return false
	}
	if this.currentTransactions[msg.Type].id == msg.Tr.Id {
		return true
	}
	return false
}

func (this *Transactions) waitToExitPending(msg *types.Message) {
	for {
		if this.checkCurrent(msg) {
			return
		}
		//@TODO - Replace with wait/notify in the future
		time.Sleep(time.Millisecond * 100)
	}
}
