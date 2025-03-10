package transaction

import (
	"github.com/saichler/layer8/go/overlay/protocol"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"google.golang.org/protobuf/proto"
	"strings"
	"sync"
	"time"
)

type TopicTransactions struct {
	cond            *sync.Cond
	pendingMap      map[string]*types.Message
	locked          *types.Message
	preCommitObject proto.Message
}

func newTopicTransactions() *TopicTransactions {
	tt := &TopicTransactions{}
	tt.pendingMap = make(map[string]*types.Message)
	tt.cond = sync.NewCond(&sync.Mutex{})
	return tt
}

func createTransaction(msg *types.Message) {
	if msg.Tr == nil {
		msg.Tr = &types.Transaction{}
		msg.Tr.Id = common.NewUuid()
		msg.Tr.StartTime = time.Now().Unix()
		msg.Tr.State = types.TransactionState_Create
	}
}

func (this *TopicTransactions) shouldHandleAsTransaction(msg *types.Message, vnic common.IVirtualNetworkInterface) (proto.Message, error, bool) {
	if msg.Action == types.Action_GET {
		this.cond.L.Lock()
		defer this.cond.L.Unlock()
		for this.locked != nil {
			this.cond.Wait()
		}
		servicePoints := vnic.Resources().ServicePoints()
		pb, err := protocol.ProtoOf(msg, vnic.Resources())
		if err != nil {
			return nil, err, false
		}
		resp, err := servicePoints.Handle(pb, msg.Action, vnic, msg, true)
		return resp, err, false
	}
	return nil, nil, true
}

func (this *TopicTransactions) addTransaction(msg *types.Message) {
	this.cond.L.Lock()
	defer this.cond.L.Unlock()
	_, ok := this.pendingMap[msg.Tr.Id]
	if ok {
		panic("Trying to add a duplicate transaction")
	}
	msg.Tr.State = types.TransactionState_Create
	this.pendingMap[msg.Tr.Id] = msg
}

func (this *TopicTransactions) finish(msg *types.Message, lock bool) {
	defer this.cond.Broadcast()
	if lock {
		this.cond.L.Lock()
		defer this.cond.L.Unlock()
	}
	if this.locked == nil {
		this.preCommitObject = nil
		return
	}
	if this.locked.Tr.Id == msg.Tr.Id {
		this.locked = nil
		this.preCommitObject = nil
	}
	delete(this.pendingMap, msg.Tr.Id)
	msg.Tr.State = types.TransactionState_Finished
}

func (this *TopicTransactions) commit(msg *types.Message, vnic common.IVirtualNetworkInterface, lock bool) bool {
	if lock {
		this.cond.L.Lock()
		defer this.cond.L.Unlock()
	}

	if msg.Tr.State != types.TransactionState_Commit {
		panic("commit: Unexpected transaction state " + msg.Tr.State.String())
	}

	if this.locked == nil {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Commit: No pending transaction"
		return false
	}

	if this.locked.Tr.Id != msg.Tr.Id {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Commit: commit is for another transaction"
		return false
	}

	if this.locked.Tr.State != types.TransactionState_Locked &&
		this.locked.Tr.State != types.TransactionState_Commit { //The state will be commit if the message hit the leader
		msg.Tr.Error = "Commit: Transaction is not in locked state " + msg.Tr.State.String()
		msg.Tr.State = types.TransactionState_Errored
		return false
	}

	if time.Now().Unix()-this.locked.Tr.StartTime >= 2 { //@TODO add the timeout
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Commit: Transaction has timed out"
		return false
	}

	servicePoints := vnic.Resources().ServicePoints()
	if msg.Action == types.Action_Notify {
		//_, err := servicePoints.Notify()
	} else {
		pb, err := protocol.ProtoOf(this.locked, vnic.Resources())
		if err != nil {
			msg.Tr.State = types.TransactionState_Errored
			msg.Tr.Error = "Commit: Protocol Error: " + err.Error()
			return false
		}
		ok := this.setPreCommitObject(msg, vnic)
		if !ok {
			msg.Tr.State = types.TransactionState_Errored
			msg.Tr.Error = "Commit: Could not set pre-commit object"
			return false
		}
		_, err = servicePoints.Handle(pb, this.locked.Action, vnic, this.locked, true)
		if err != nil {
			msg.Tr.State = types.TransactionState_Errored
			msg.Tr.Error = "Commit: Handle Error: " + err.Error()
			return false
		}
		this.locked.Tr.State = types.TransactionState_Commited
	}

	msg.Tr.State = types.TransactionState_Commited
	return true
}

func (this *TopicTransactions) setPreCommitObject(msg *types.Message, vnic common.IVirtualNetworkInterface) bool {

	pb, err := protocol.ProtoOf(this.locked, vnic.Resources())
	if err != nil {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Pre Commit Object Fetch: Protocol Error: " + err.Error()
		return false
	}

	if msg.Action == types.Action_PUT ||
		msg.Action == types.Action_DELETE ||
		msg.Action == types.Action_PATCH {
		servicePoints := vnic.Resources().ServicePoints()
		//Get the object before performing the action so we could rollback
		//if necessary.
		resp, e := servicePoints.Handle(pb, types.Action_GET, vnic, this.locked, true)
		if e != nil {
			msg.Tr.State = types.TransactionState_Errored
			msg.Tr.Error = "Pre Commit Object Fetch: Service Point: " + e.Error()
			return false
		}
		this.preCommitObject = resp
	} else {
		this.preCommitObject = pb
	}
	return true
}

func (this *TopicTransactions) setRollbackAction(msg *types.Message) {
	switch msg.Action {
	case types.Action_POST:
		this.locked.Action = types.Action_DELETE
	case types.Action_DELETE:
		this.locked.Action = types.Action_POST
	case types.Action_PUT:
		this.locked.Action = types.Action_PUT
	case types.Action_PATCH:
		this.locked.Action = types.Action_PUT
	}
}

func (this *TopicTransactions) rollback(msg *types.Message, vnic common.IVirtualNetworkInterface, lock bool) bool {
	if lock {
		this.cond.L.Lock()
		defer this.cond.L.Unlock()
	}

	if msg.Tr.State != types.TransactionState_Rollback {
		panic("commit: Unexpected transaction state " + msg.Tr.State.String())
	}

	if this.locked == nil {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Rollback: No committed transaction"
		return false
	}

	if this.locked.Tr.Id != msg.Tr.Id {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Rollback: commit was for another transaction"
		return false
	}

	if this.locked.Tr.State != types.TransactionState_Commited {
		msg.Tr.Error = "Rollback: Transaction is not in committed state " + msg.Tr.State.String()
		msg.Tr.State = types.TransactionState_Errored
		return false
	}

	servicePoints := vnic.Resources().ServicePoints()
	if msg.Action == types.Action_Notify {
		//_, err := servicePoints.Notify()
	} else {
		this.setRollbackAction(msg)
		_, err := servicePoints.Handle(this.preCommitObject, this.locked.Action, vnic, this.locked, true)
		if err != nil {
			msg.Tr.State = types.TransactionState_Errored
			msg.Tr.Error = "Rollback: Handle Error: " + err.Error()
			return false
		}
	}

	msg.Tr.State = types.TransactionState_Rollbacked
	return true
}

func (this *TopicTransactions) lock(msg *types.Message, lock bool) bool {
	if lock {
		this.cond.L.Lock()
		defer this.cond.L.Unlock()
	}

	if msg.Tr.State != types.TransactionState_Lock {
		panic("lock: Unexpected transaction state " + msg.Tr.State.String())
	}

	if this.locked == nil {
		m := this.pendingMap[msg.Tr.Id]
		if m == nil {
			panic("Can't find message " + msg.Tr.Id)
		}
		this.locked = m
		msg.Tr.State = types.TransactionState_Locked
		m.Tr.State = msg.Tr.State
		return true
	} else if this.locked.Tr.Id != msg.Tr.Id &&
		this.locked.Tr.State != types.TransactionState_Locked &&
		strings.Compare(this.locked.Tr.Id, msg.Tr.Id) == -1 {
		m := this.pendingMap[msg.Tr.Id]
		if m == nil {
			panic("Can't find message " + msg.Tr.Id)
		}
		this.locked = m
		msg.Tr.State = types.TransactionState_Locked
		m.Tr.State = msg.Tr.State
		return true
	}

	msg.Tr.State = types.TransactionState_LockFailed
	msg.Tr.Error = "Failed to lock : " + msg.Topic
	return false
}
