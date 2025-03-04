package service_points

import (
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
	"google.golang.org/protobuf/proto"
	"sync"
)

type Transactions struct {
	currentTransactions map[string]*Transaction
	pendingTransactions map[string][]*Transaction
	cond                *sync.Cond
	pendingPeerRequests map[string]*PeerRequests
}

type PeerRequests struct {
	trId         string
	pending      map[string]bool
	pendingCount int
}

type Transaction struct {
	id        string
	action    types.Action
	pb        proto.Message
	handler   interfaces.IServicePointHandler
	startTime int64
	state     types.TrState
	cond      *sync.Cond
}

func newTransactions() *Transactions {
	trs := &Transactions{}
	trs.currentTransactions = make(map[string]*Transaction)
	trs.cond = sync.NewCond(&sync.Mutex{})
	trs.pendingTransactions = make(map[string][]*Transaction)
	return trs
}

func (this *Transactions) startTransactions(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) proto.Message {
	ok, tr := this.createTransaction(msg, vnic.Resources())
	if !ok {
		return msg.Tr
	}

	tr.cond.L.Lock()
	for tr.state == types.TrState_Pending {
		tr.cond.Wait()
	}
	tr.cond.L.Unlock()

	ok = this.topicLock(msg, vnic)
	if !ok {
		msg.Tr.State = types.TrState_Errored
		msg.Tr.Error = "Lock: Failed to acquire topic lock"
		_, nextTr := this.localClean(msg)
		notifyNextTR(nextTr)
		return msg.Tr
	}

	isLeader, leaderUuid := IsLeader(vnic.Resources(), vnic.Resources().Config().LocalUuid, msg.Type, msg.Vlan)

	msg.Tr.State = types.TrState_Commit

	if !isLeader {
		r, _ := vnic.Forward(msg, leaderUuid)
		resp, _ := r.(proto.Message)
		return resp
	}

	return this.runTransaction(msg, vnic)
}

func (this *Transactions) runTransaction(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) proto.Message {
	switch msg.Tr.State {
	case types.TrState_Commit:
		return this.localCommit(msg, vnic)
	case types.TrState_Lock:
		return this.localLock(msg, vnic.Resources())
	case types.TrState_Rollback:
		return this.localRollback(msg)
	case types.TrState_Clean:
		pb, tr := this.localClean(msg)
		notifyNextTR(tr)
		return pb
	case types.TrState_Errored:
		return msg.Tr
	default:
		panic("Unexpected transaction state " + msg.Tr.State.String() + ":" + msg.Tr.Error)
	}
}
