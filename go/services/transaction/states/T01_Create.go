package states

import (
	"context"
	"runtime"
	"time"

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

	//To Keep the same flow, we are going to forward the transaction to the leader
	//even if this is the leader
	go func() {
		runtime.Gosched()
		leader := vnic.Resources().Services().GetLeader(msg.ServiceName(), msg.ServiceArea())
		leaderResponse := vnic.Forward(msg, leader)
		//Send the final resulth to the initiator.
		vnic.Reply(msg, leaderResponse)
	}()

	//Return the temporary response as the transaction state created
	return L8TransactionFor(msg)
}

func (this *TransactionManager) Create2(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	createTransaction(msg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	go func() {
		defer cancel()

		select {
		case <-ctx.Done():
			vnic.Resources().Logger().Warning("Transaction forwarding cancelled")
			return
		default:
			leader := vnic.Resources().Services().GetLeader(msg.ServiceName(), msg.ServiceArea())
			if leader == "" {
				vnic.Resources().Logger().Error("No leader found for service")
				return
			}

			leaderResponse := vnic.Forward(msg, leader)
			if err := vnic.Reply(msg, leaderResponse); err != nil {
				vnic.Resources().Logger().Error("Failed to reply:", err)
			}
		}
	}()

	return L8TransactionFor(msg)
}
