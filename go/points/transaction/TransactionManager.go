package transaction

import (
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
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

func (this *TransactionManager) Start(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) (proto.Message, error) {
	createTransaction(msg)
	tt := this.topicTransaction(msg)
	tt.addTransaction(msg)

	ok := requestFromAllPeers(msg, vnic)
	if !ok {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Failed to create transaction"
		return msg.Tr, nil
	}

	msg.Tr.State = types.TransactionState_Start
	healthCenter := health.Health(vnic.Resources())
	leader := healthCenter.Leader(msg.Type, msg.Vlan)
	isLeader := leader == vnic.Resources().Config().LocalUuid
	if !isLeader {
		resp, err := vnic.Forward(msg, leader)
		return resp.(proto.Message), err
	}

	this.start(msg, vnic)
	return msg.Tr, nil
}

func (this *TransactionManager) Run(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) (proto.Message, error) {
	switch msg.Tr.State {
	case types.TransactionState_Create:
		this.create(msg)
	case types.TransactionState_Start:
		this.start(msg, vnic)
	case types.TransactionState_Lock:
		this.lock(msg)
	case types.TransactionState_Commit:
		this.commit(msg, vnic)
	case types.TransactionState_Commited:
		this.commited(msg)
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

func (this *TransactionManager) start(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) {
	tt := this.topicTransaction(msg)
	tt.mtx.Lock()
	defer tt.mtx.Unlock()

	if msg.Tr.State != types.TransactionState_Start {
		panic("start: Unexpected transaction state " + msg.Tr.State.String())
	}

	healthCenter := health.Health(vnic.Resources())
	isLeader := healthCenter.Leader(msg.Type, msg.Vlan) == vnic.Resources().Config().LocalUuid
	if !isLeader {
		msg.Tr.State = types.TransactionState_Errored
		msg.Tr.Error = "Start transaction invoked on a follower"
	}

	//Try to lock on all the followers
	msg.Tr.State = types.TransactionState_Lock
	ok := requestFromAllPeers(msg, vnic)
	if !ok {
		return
	}

	//now try to lock on the leader
	msg.Tr.State = types.TransactionState_Lock
	ok = tt.lock(msg, false)
	//We were not able to lock on the leader
	if !ok {
		return
	}

	//At this point we are ready to commit
	//Try to commit on the followers
	msg.Tr.State = types.TransactionState_Commit
	ok = requestFromAllPeers(msg, vnic)
	if !ok {

		return
	}

	//Try to commit on the leader
	msg.Tr.State = types.TransactionState_Commit
	ok = tt.commit(msg, vnic, false)
	if !ok {

	}

	//Cleanup and release the lock
	requestFromAllPeers(msg, vnic)
	tt.commited(msg, false)
}

func (this *TransactionManager) lock(msg *types.Message) {
	tt := this.topicTransaction(msg)
	tt.lock(msg, true)
}

func (this *TransactionManager) commit(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) {
	tt := this.topicTransaction(msg)
	tt.commit(msg, vnic, true)
}

func (this *TransactionManager) commited(msg *types.Message) {
	tt := this.topicTransaction(msg)
	tt.commited(msg, true)
}
