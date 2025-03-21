package transaction

import (
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/layer8/go/overlay/protocol"
	"github.com/saichler/reflect/go/reflect/cloning"
	"github.com/saichler/serializer/go/serialize/serializers"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"google.golang.org/protobuf/proto"
	"sync"
)

type TransactionManager struct {
	serviceTransactions map[string]*ServiceTransactions
	mtx                 *sync.Mutex
}

func NewTransactionManager() *TransactionManager {
	tm := &TransactionManager{}
	tm.mtx = &sync.Mutex{}
	tm.serviceTransactions = make(map[string]*ServiceTransactions)
	return tm
}

func (this *TransactionManager) transactionsOf(msg *types.Message) *ServiceTransactions {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	serviceKey := ServiceKey(msg.ServiceName, msg.ServiceArea)
	st, ok := this.serviceTransactions[serviceKey]
	if !ok {
		this.serviceTransactions[serviceKey] = newTransactionsForMulticast()
		st = this.serviceTransactions[serviceKey]
	}
	return st
}

func (this *TransactionManager) Start(msg *types.Message, vnic common.IVirtualNetworkInterface) (proto.Message, error) {
	st := this.transactionsOf(msg)

	//This is a Get request, needs to be handled outside a transaction
	resp, err, ok := st.shouldHandleAsTransaction(msg, vnic)
	if !ok {
		return resp, err
	}

	createTransaction(msg)
	st.addTransaction(msg)
	healthCenter := health.Health(vnic.Resources())
	targets := healthCenter.Uuids(msg.ServiceName, msg.ServiceArea, true)
	delete(targets, vnic.Resources().Config().LocalUuid)

	ok, _ = requestFromPeers(msg, vnic, targets)
	if !ok {
		//Cleanup as we failed
		msg.Tr.State = types.TransactionState_Finish
		requestFromPeers(msg, vnic, targets)
		st.finish(msg, true)

		//Mark as error
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Failed to create transaction"
		return msg.Tr, nil
	}

	msg.Tr.State = types.TransactionState_Start
	leader := healthCenter.Leader(msg.ServiceName, msg.ServiceArea)
	isLeader := leader == vnic.Resources().Config().LocalUuid

	//from this point onwards, we are going to use a clone
	//As we only need the message attributes, without the data
	msgClone := cloning.NewCloner().Clone(msg).(*types.Message)
	msgClone.Data, _ = protocol.DataFor(&types.Transaction{}, &serializers.ProtoBuffBinary{}, vnic.Resources().Security())

	if !isLeader {
		resp, err := vnic.Forward(msgClone, leader)
		return resp.(proto.Message), err
	}

	this.start(msgClone, vnic)
	return msgClone.Tr, nil
}

func (this *TransactionManager) Run(msg *types.Message, vnic common.IVirtualNetworkInterface) (proto.Message, error) {
	switch msg.Tr.State {
	case types.TransactionState_Create:
		this.create(msg)
	case types.TransactionState_Start:
		this.start(msg, vnic)
	case types.TransactionState_Lock:
		this.lock(msg)
	case types.TransactionState_Commit:
		this.commit(msg, vnic)
	case types.TransactionState_Finish:
		this.finish(msg)
	case types.TransactionState_Rollback:
		this.rollback(msg, vnic)
	case types.TransactionState_Errored:
	default:
		panic("Unexpected transaction state " + msg.Tr.State.String() + ":" + msg.Tr.Error)
	}
	return msg.Tr, nil
}

func (this *TransactionManager) create(msg *types.Message) {
	if msg.Tr.State != types.TransactionState_Create {
		panic("create: Unexpected transaction state " + msg.Tr.State.String())
	}
	createTransaction(msg)
	st := this.transactionsOf(msg)
	st.addTransaction(msg)
	msg.Tr.State = types.TransactionState_Created
}

func (this *TransactionManager) Targets(msg *types.Message, vnic common.IVirtualNetworkInterface) (bool, bool, map[string]bool, map[string]bool) {
	healthCenter := health.Health(vnic.Resources())
	isLeader := healthCenter.Leader(msg.ServiceName, msg.ServiceArea) == vnic.Resources().Config().LocalUuid
	targets := healthCenter.Uuids(msg.ServiceName, msg.ServiceArea, true)
	replicas := make(map[string]bool)
	for target, _ := range targets {
		replicas[target] = true
	}
	isLeaderATarget := true

	//If this is a replication count transaction and the action type is POST,
	//Find out which of the targets need to be included in the commit.
	servicePoint, _ := vnic.Resources().ServicePoints().ServicePointHandler(msg.ServiceName, msg.ServiceArea)
	if servicePoint.ReplicationCount() > 0 && msg.Action == types.Action_POST {
		reps := healthCenter.ReplicasFor(msg.ServiceName, msg.ServiceArea, servicePoint.ReplicationCount())
		replicas = make(map[string]bool)
		for target, _ := range reps {
			replicas[target] = true
		}
		// Is the leader elected to be part of this commit
		_, isLeaderATarget = replicas[vnic.Resources().Config().LocalUuid]
	}

	//Remove the leader from the targets & the replicas
	delete(targets, vnic.Resources().Config().LocalUuid)
	delete(replicas, vnic.Resources().Config().LocalUuid)

	return isLeader, isLeaderATarget, targets, replicas
}

func (this *TransactionManager) start(msg *types.Message, vnic common.IVirtualNetworkInterface) {
	//Get the states from the health instance
	isLeader, isLeaderATarget, targets, replicas := this.Targets(msg, vnic)

	//Get the topic transactions and lock it
	st := this.transactionsOf(msg)
	st.cond.L.Lock()
	defer func() {
		//Cleanup
		oldState := msg.Tr.State
		msg.Tr.State = types.TransactionState_Finish
		requestFromPeers(msg, vnic, targets)
		st.finish(msg, false)
		msg.Tr.State = oldState
		st.cond.L.Unlock()
	}()

	//If the state isn't Start, this means there is a major bug so panic
	if msg.Tr.State != types.TransactionState_Start {
		panic("start: Unexpected transaction state " + msg.Tr.State.String())
	}

	//There is a race condition, if the leader has changed during this transaction
	//Fail it
	if !isLeader {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Start transaction invoked on a follower"
		return
	}

	//Try to lock on all the followers
	msg.Tr.State = types.TransactionState_Lock
	ok, _ := requestFromPeers(msg, vnic, targets)
	if !ok {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Failed to lock followers"
		return
	}

	//now try to lock on the leader
	msg.Tr.State = types.TransactionState_Lock
	ok = st.lock(msg, false)
	//We were not able to lock on the leader
	if !ok {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Failed to lock leader"
		return
	}

	//At this point we are ready to commit
	//Try to commit on the followers
	//Note we do it on the replicas and not on targets as if this is a replication
	//count commit, we want to commit only on the replicas
	msg.Tr.State = types.TransactionState_Commit
	ok, peers := requestFromPeers(msg, vnic, replicas)
	if !ok {
		//Request a rollback only from those peers that commited
		msg.Tr.State = types.TransactionState_Rollback
		rollTarget := make(map[string]bool)
		for target, e := range peers {
			if e == "" {
				rollTarget[target] = true
			}
		}
		requestFromPeers(msg, vnic, rollTarget)

		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Followers failed to commit"
		return
	}

	//Try to commit on the leader, if you need to
	if isLeaderATarget {
		msg.Tr.State = types.TransactionState_Commit
		ok = st.commit(msg, vnic, false)
		if !ok {
			//Request a rollback from the followers
			msg.Tr.State = types.TransactionState_Rollback
			requestFromPeers(msg, vnic, replicas)

			errorMsg := "Leader failed to commit"
			if !ok {
				errorMsg = "Leader failed to commit and failed to clean up"
			}
			msg.Tr.State = types.TransactionState_Errored
			msg.Tr.Error = errorMsg
			return
		}
	}
	//Cleanup and release the lock
	msg.Tr.State = types.TransactionState_Commited
}

func (this *TransactionManager) lock(msg *types.Message) {
	st := this.transactionsOf(msg)
	st.lock(msg, true)
}

func (this *TransactionManager) commit(msg *types.Message, vnic common.IVirtualNetworkInterface) {
	st := this.transactionsOf(msg)
	st.commit(msg, vnic, true)
}

func (this *TransactionManager) rollback(msg *types.Message, vnic common.IVirtualNetworkInterface) {
	st := this.transactionsOf(msg)
	st.rollback(msg, vnic, true)
}

func (this *TransactionManager) finish(msg *types.Message) {
	st := this.transactionsOf(msg)
	st.finish(msg, true)
}
