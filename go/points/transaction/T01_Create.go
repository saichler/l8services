package transaction

import (
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/layer8/go/overlay/protocol"
	"github.com/saichler/reflect/go/reflect/cloning"
	"github.com/saichler/serializer/go/serialize/serializers"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"time"
)

func createTransaction(msg *types.Message) {
	if msg.Tr == nil {
		msg.Tr = &types.Transaction{}
		msg.Tr.Id = common.NewUuid()
		msg.Tr.StartTime = time.Now().Unix()
		msg.Tr.State = types.TransactionState_Create
	}
}

func (this *TransactionManager) Create(msg *types.Message, vnic common.IVirtualNetworkInterface) common.IMObjects {
	st := this.transactionsOf(msg)

	//This is a Get request, needs to be handled outside a transaction
	resp, ok := st.shouldHandleAsTransaction(msg, vnic)
	if !ok {
		return resp
	}

	//Create the new transaction inside the message
	createTransaction(msg)

	//Add transaction to the local service transaction map
	st.addTransaction(msg)

	//Compile a list of this service peers, takeaway this instance
	healthCenter := health.Health(vnic.Resources())
	targets := healthCenter.Uuids(msg.ServiceName, msg.ServiceArea, true)
	delete(targets, vnic.Resources().Config().LocalUuid)

	//Send the new transaction message to all the peers.
	ok, _ = requestFromPeers(msg, vnic, targets)
	if !ok {
		//One or more peers did not accept/created the transaction
		//in its map, so cleanup
		msg.Tr.State = types.TransactionState_Finish
		requestFromPeers(msg, vnic, targets)
		st.delTransaction(msg)
		msg.Tr.Error = "Failed to create transaction"
		return response.New(nil, msg.Tr)
	}

	//Move the transaction state to start and find the leader
	msg.Tr.State = types.TransactionState_Start
	leader := healthCenter.Leader(msg.ServiceName, msg.ServiceArea)
	isLeader := leader == vnic.Resources().Config().LocalUuid

	//from this point onwards, we are going to use a clone
	//As we only need the message attributes, without the data
	msgClone := cloning.NewCloner().Clone(msg).(*types.Message)
	msgClone.Data, _ = protocol.DataFor(&types.Transaction{}, &serializers.ProtoBuffBinary{}, vnic.Resources().Security())

	//If this is not the leader, forward to the leader
	if !isLeader {
		response := vnic.Forward(msgClone, leader)
		return response
	}

	this.start(msgClone, vnic)
	return response.New(nil, msgClone.Tr)
}
