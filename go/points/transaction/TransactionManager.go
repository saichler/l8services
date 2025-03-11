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
	topics map[string]*TopicTransactions
	mtx    *sync.Mutex
}

func NewTransactionManager() *TransactionManager {
	tm := &TransactionManager{}
	tm.mtx = &sync.Mutex{}
	tm.topics = make(map[string]*TopicTransactions)
	return tm
}

func (this *TransactionManager) topicTransaction(msg *types.Message) *TopicTransactions {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	tt, ok := this.topics[msg.Type]
	if !ok {
		this.topics[msg.Type] = newTopicTransactions()
		tt = this.topics[msg.Type]
	}
	return tt
}

func (this *TransactionManager) Start(msg *types.Message, vnic common.IVirtualNetworkInterface) (proto.Message, error) {
	tt := this.topicTransaction(msg)

	//This is a Get request, needs to be handled outside a transaction
	resp, err, ok := tt.shouldHandleAsTransaction(msg, vnic)
	if !ok {
		return resp, err
	}

	createTransaction(msg)
	tt.addTransaction(msg)

	ok, _ = requestFromAllPeers(msg, vnic)
	if !ok {
		//Cleanup as we failed
		msg.Tr.State = types.TransactionState_Commited
		requestFromAllPeers(msg, vnic)
		//Mark as error
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Failed to create transaction"
		return msg.Tr, nil
	}

	msg.Tr.State = types.TransactionState_Start
	healthCenter := health.Health(vnic.Resources())
	leader := healthCenter.Leader(msg.Type, msg.Vlan)
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
	tt := this.topicTransaction(msg)
	tt.addTransaction(msg)
	msg.Tr.State = types.TransactionState_Created
}

func (this *TransactionManager) start(msg *types.Message, vnic common.IVirtualNetworkInterface) {
	tt := this.topicTransaction(msg)
	tt.cond.L.Lock()
	defer func() {
		//Cleanup
		oldState := msg.Tr.State
		msg.Tr.State = types.TransactionState_Finish
		requestFromAllPeers(msg, vnic)
		tt.finish(msg, false)
		msg.Tr.State = oldState
		tt.cond.L.Unlock()
	}()

	if msg.Tr.State != types.TransactionState_Start {
		panic("start: Unexpected transaction state " + msg.Tr.State.String())
	}

	healthCenter := health.Health(vnic.Resources())
	isLeader := healthCenter.Leader(msg.Type, msg.Vlan) == vnic.Resources().Config().LocalUuid
	if !isLeader {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Start transaction invoked on a follower"
		return
	}

	//Try to lock on all the followers
	msg.Tr.State = types.TransactionState_Lock
	ok, _ := requestFromAllPeers(msg, vnic)
	if !ok {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Failed to lock followers"
		return
	}

	//now try to lock on the leader
	msg.Tr.State = types.TransactionState_Lock
	ok = tt.lock(msg, false)
	//We were not able to lock on the leader
	if !ok {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Failed to lock leader"
		return
	}

	//At this point we are ready to commit
	//Try to commit on the followers
	msg.Tr.State = types.TransactionState_Commit
	ok, peers := requestFromAllPeers(msg, vnic)
	if !ok {
		//Request a rollback only from those peers that commited
		msg.Tr.State = types.TransactionState_Rollback
		requestFromPeers(msg, vnic, peers)

		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Followers failed to commit"
		return
	}

	//Try to commit on the leader
	msg.Tr.State = types.TransactionState_Commit
	ok = tt.commit(msg, vnic, false)
	if !ok {
		//Request a rollback from the followers
		msg.Tr.State = types.TransactionState_Rollback
		requestFromAllPeers(msg, vnic)

		errorMsg := "Leader failed to commit"
		if !ok {
			errorMsg = "Leader failed to commit and failed to clean up"
		}
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = errorMsg
		return
	}

	//Cleanup and release the lock
	msg.Tr.State = types.TransactionState_Commited
}

func (this *TransactionManager) lock(msg *types.Message) {
	tt := this.topicTransaction(msg)
	tt.lock(msg, true)
}

func (this *TransactionManager) commit(msg *types.Message, vnic common.IVirtualNetworkInterface) {
	tt := this.topicTransaction(msg)
	tt.commit(msg, vnic, true)
}

func (this *TransactionManager) rollback(msg *types.Message, vnic common.IVirtualNetworkInterface) {
	tt := this.topicTransaction(msg)
	tt.rollback(msg, vnic, true)
}

func (this *TransactionManager) finish(msg *types.Message) {
	tt := this.topicTransaction(msg)
	tt.finish(msg, true)
}
