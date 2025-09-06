package states

import (
	"time"

	"github.com/saichler/l8services/go/services/transaction/requests"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types"
	"github.com/saichler/layer8/go/overlay/protocol"
)

func createTransaction(msg *ifs.Message) {
	if msg.Tr_State() == ifs.Empty {
		msg.SetTr_Id(ifs.NewUuid())
		msg.SetTr_State(ifs.Create)
		msg.SetTr_StartTime(time.Now().Unix())
	}
}

func (this *TransactionManager) Create(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	st := this.transactionsOf(msg)

	//This is a Get request, needs to be handled outside a transaction
	resp, ok := st.shouldHandleAsTransaction(msg, vnic)
	if !ok {
		return resp
	}

	//Create the new transaction inside the message
	createTransaction(msg)

	//Add transaction to the local service transaction map
	tr := st.addTransaction(msg, vnic)
	tr.Debug("T01_Create.Create: Created Transaction ", msg.Tr_Id(), " in ", vnic.Resources().SysConfig().LocalUuid)

	//Compile a list of this service peers, takeaway this instance
	targets := tr.Targets()
	tr.Msg().SetRequestReply(true, false)
	//Send the new transaction message to all the peers.
	ok, _ = requests.RequestFromPeers(tr, targets)
	if !ok {
		//One or more peers did not accept/created the transaction
		//in its map, so cleanup
		tr.Msg().SetTr_State(ifs.Finish)
		requests.RequestFromPeers(tr, targets)
		st.delTransaction(tr.Msg(), ifs.Errored)
		tr.Msg().SetTr_ErrMsg("T01_Create.Create: RequestFromPeers failed, failed to create transaction " + tr.Msg().Tr_Id())
		tr.Error(tr.Msg().Tr_ErrMsg())
		return object.New(nil, TransactionOf(tr))
	}

	//Move the transaction state to start and find the leader
	tr.Msg().SetTr_State(ifs.Start)

	//from this point onwards, we are going to use a clone
	//As we only need the message attributes, without the data
	msgClone := tr.Msg().Clone()
	o := object.New(nil, &types.Transaction{})
	data, _ := protocol.DataFor(o, tr.Security())
	msgClone.SetData(data)

	//We don't want to favor the leader so we are sending the message although this might be the leader
	//if !tr.IsLeader() {
	tr.Msg().SetTr_State(ifs.Created)
	tr.Debug("T01_Create.Create: Forwarding transaction to leader ", tr.Msg().Tr_Id())
	response := vnic.Forward(msgClone, tr.Leader())
	tr.Debug("T01_Create.Create: Received response from leader ", tr.Msg().Tr_Id())
	return response
	//}
	//tr.Debug("T01_Create.Create: Transaction created & started from leader ", tr.Msg().Tr_Id())
	//this.start(msgClone, vnic)
	//return object.New(nil, TransactionFor(msgClone))
}
