package states

import (
	"sync"

	"github.com/saichler/l8types/go/ifs"
)

func createTransaction(msg *ifs.Message) {
	if msg.Tr_State() == ifs.NotATransaction {
		msg.SetTr_Id(ifs.NewUuid())
		msg.SetTr_State(ifs.Created)
	}
}

func (this *TransactionManager) Create(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	//Create the new transaction inside the message
	createTransaction(msg)

	mtx := sync.Mutex{}
	mtx.Lock()
	defer mtx.Unlock()
	//To Keep the same flow, we are going to forward the transaction to the leader
	//even if this is the leader
	go func() {
		mtx.Lock()
		defer mtx.Unlock()
		leader := vnic.Resources().Services().GetLeader(msg.ServiceName(), msg.ServiceArea())
		leaderResponse := vnic.Forward(msg, leader)
		//Send the final resulth to the initiator.
		vnic.Reply(msg, leaderResponse)
	}()
	//Return the temporary response as the transaction state created
	return L8TransactionFor(msg)
}
