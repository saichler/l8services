package transaction

import (
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/layer8/go/overlay/protocol"
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/types/go/common"
)

func createTransaction(msg common.IMessage) {
	if common.IsNil(msg.Tr()) {
		msg.SetTr(protocol.NewTransaction())
	}
}

func (this *TransactionManager) Create(msg common.IMessage, vnic common.IVirtualNetworkInterface) common.IElements {
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
	targets := healthCenter.Uuids(msg.ServiceName(), msg.ServiceArea())
	delete(targets, vnic.Resources().SysConfig().LocalUuid)

	//Send the new transaction message to all the peers.
	ok, _ = requestFromPeers(msg, vnic, targets)
	if !ok {
		//One or more peers did not accept/created the transaction
		//in its map, so cleanup
		msg.Tr().SetState(common.Finish)
		requestFromPeers(msg, vnic, targets)
		st.delTransaction(msg)
		msg.Tr().SetErrorMessage("Failed to create transaction")
		return object.New(nil, msg.Tr)
	}

	//Move the transaction state to start and find the leader
	msg.Tr().SetState(common.Start)
	leader := healthCenter.Leader(msg.ServiceName(), msg.ServiceArea())
	isLeader := leader == vnic.Resources().SysConfig().LocalUuid

	//from this point onwards, we are going to use a clone
	//As we only need the message attributes, without the data
	msgClone := msg.(*protocol.Message).Clone()
	o := object.New(nil, &protocol.Transaction{})
	data, _ := protocol.DataFor(o, vnic.Resources().Security())
	msgClone.SetData(data)

	//If this is not the leader, forward to the leader
	if !isLeader {
		response := vnic.Forward(msgClone, leader)
		return response
	}

	this.start(msgClone, vnic)
	return object.New(nil, msgClone.Tr())
}
