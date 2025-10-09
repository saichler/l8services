package states

import (
	"github.com/saichler/l8services/go/services/transaction/requests"
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceTransactions) run(msg *ifs.Message) {
	this.nic.Resources().Logger().Debug("T02_Run.run: ", msg.Tr_Id(), " for ServiceName ", msg.ServiceName(), " area ", msg.ServiceArea())
	//Check if this is the leader, again, just to make sure
	if this.nic.Resources().Services().GetLeader(msg.ServiceName(), msg.ServiceArea()) != this.nic.Resources().SysConfig().LocalUuid {
		msg.SetTr_State(ifs.Failed)
		msg.SetTr_ErrMsg("A non leader has got the message")
		this.nic.Reply(msg, L8TransactionFor(msg))
		return
	}

	//notify the originator that the transaction is running
	msg.SetTr_State(ifs.Running)
	this.nic.Reply(msg, L8TransactionFor(msg))

	var targets map[string]bool
	service, _ := this.nic.Resources().Services().ServiceHandler(msg.ServiceName(), msg.ServiceArea())
	if service.TransactionConfig().Replication() {
		targets = this.nic.Resources().Services().RoundRobinParticipants(msg.ServiceName(), msg.ServiceArea(),
			service.TransactionConfig().ReplicationCount())
	} else {
		targets = this.nic.Resources().Services().GetParticipants(msg.ServiceName(), msg.ServiceArea())
	}

	this.nic.Resources().Logger().Debug("T02_Run.run: Sending to targets", msg.Tr_Id())
	ok, peers := requests.RequestFromPeers(msg, targets, this.nic)
	if !ok {
		commitedTargets := make(map[string]bool)
		errMsg := ""
		for k, v := range peers {
			if v == "" {
				commitedTargets[k] = true
			} else {
				errMsg = v
			}
		}

		// Send Rollback only to those peers that commited successfully
		msg.SetTr_State(ifs.Rollback)
		requests.RequestFromPeers(msg, commitedTargets, this.nic)

		msg.SetTr_State(ifs.Failed)
		msg.SetTr_ErrMsg("T02_Run.run: Failed to commit:" + errMsg)

		this.nic.Reply(msg, L8TransactionFor(msg))
		return
	}
	this.nic.Resources().Logger().Debug("T02_Run.run: Transaction committed: ", msg.Tr_Id())
	msg.SetTr_State(ifs.Committed)
	this.nic.Reply(msg, L8TransactionFor(msg))

	//cleanup
	msg.SetTr_State(ifs.Cleanup)
	requests.RequestFromPeers(msg, targets, this.nic)
}
