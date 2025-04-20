package transaction

import (
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/servicepoints/go/points/transaction/requests"
	"github.com/saichler/types/go/common"
	"sync"
)

func (this *ServiceTransactions) run(msg common.IMessage, vnic common.IVirtualNetworkInterface, cond *sync.Cond) common.ITransaction {
	isLeader, isLeaderATarget, targets, replicas := Targets(msg, vnic)
	cond.L.Lock()
	defer func() {
		defer cond.L.Unlock()
		vnic.Resources().Logger().Debug("Tr Leader Cleanup")
		//Cleanup
		oldState := msg.Tr().State()
		msg.Tr().SetState(common.Finish)
		requests.RequestFromPeers(msg, vnic, targets)
		this.finish(msg)
		msg.Tr().SetState(oldState)
		cond.Broadcast()
		vnic.Resources().Logger().Debug("Tr Leader Cleanup finished")
	}()

	//If the state isn't Start, this means there is a major bug so panic
	if msg.Tr().State() != common.Start {
		panic("start: Unexpected transaction state " + msg.Tr().State().String())
	}

	//There is a race condition, if the leader has changed during this transaction
	//Fail it
	if !isLeader {
		msg.Tr().SetState(common.Errored)
		msg.Tr().SetErrorMessage("Start transaction invoked on a follower")
		return msg.Tr()
	}

	vnic.Resources().Logger().Debug("Tr Leader Lock followers")

	//Try to lock on all the followers
	msg.Tr().SetState(common.Lock)
	ok, _ := requests.RequestFromPeers(msg, vnic, targets)
	if !ok {
		msg.Tr().SetState(common.Errored)
		msg.Tr().SetErrorMessage("Failed to lock followers")
		return msg.Tr()
	}

	vnic.Resources().Logger().Debug("Tr Leader Lock leader")

	//now try to lock on the leader
	msg.Tr().SetState(common.Lock)
	ok = this.lock(msg)
	//We were not able to lock on the leader
	if !ok {
		msg.Tr().SetState(common.Errored)
		msg.Tr().SetErrorMessage("Failed to lock leader")
		return msg.Tr()
	}

	vnic.Resources().Logger().Debug("Tr Leader Commit followers")

	//At this point we are ready to commit
	//Try to commit on the followers
	//Note we do it on the replicas and not on targets as if this is a replication
	//count commit, we want to commit only on the replicas
	msg.Tr().SetState(common.Commit)
	ok, peers := requests.RequestFromPeers(msg, vnic, replicas)
	if !ok {
		//Request a rollback only from those peers that commited
		msg.Tr().SetState(common.Rollback)
		rollTarget := make(map[string]bool)
		for target, e := range peers {
			if e == "" {
				rollTarget[target] = true
			}
		}
		requests.RequestFromPeers(msg, vnic, rollTarget)

		msg.Tr().SetState(common.Errored)
		msg.Tr().SetErrorMessage("Followers failed to commit")
		return msg.Tr()
	}

	vnic.Resources().Logger().Debug("Tr Leader Commit leader")

	//Try to commit on the leader, if you need to
	if isLeaderATarget {
		msg.Tr().SetState(common.Commit)
		ok = this.commit(msg, vnic)
		if !ok {
			//Request a rollback from the followers
			msg.Tr().SetState(common.Rollback)
			requests.RequestFromPeers(msg, vnic, replicas)

			errorMsg := "Leader failed to commit"
			if !ok {
				errorMsg = "Leader failed to commit and failed to clean up"
			}
			msg.Tr().SetState(common.Errored)
			msg.Tr().SetErrorMessage(errorMsg)
			return msg.Tr()
		}
	}

	vnic.Resources().Logger().Debug("Tr Leader Commited")

	//Cleanup and release the lock
	msg.Tr().SetState(common.Commited)
	return msg.Tr()
}

func Targets(msg common.IMessage, vnic common.IVirtualNetworkInterface) (bool, bool, map[string]bool, map[string]bool) {
	healthCenter := health.Health(vnic.Resources())
	isLeader := healthCenter.Leader(msg.ServiceName(), msg.ServiceArea()) == vnic.Resources().SysConfig().LocalUuid
	targets := healthCenter.Uuids(msg.ServiceName(), msg.ServiceArea())
	replicas := make(map[string]bool)
	for target, _ := range targets {
		replicas[target] = true
	}
	isLeaderATarget := true

	//If this is a replication count transaction and the action type is POST,
	//Find out which of the targets need to be included in the commit.
	servicePoint, _ := vnic.Resources().ServicePoints().ServicePointHandler(msg.ServiceName(), msg.ServiceArea())
	if servicePoint.ReplicationCount() > 0 && msg.Action() == common.POST {
		reps := healthCenter.ReplicasFor(msg.ServiceName(), msg.ServiceArea(), servicePoint.ReplicationCount())
		replicas = make(map[string]bool)
		for target, _ := range reps {
			replicas[target] = true
		}
		// Is the leader elected to be part of this commit
		_, isLeaderATarget = replicas[vnic.Resources().SysConfig().LocalUuid]
	}

	//Remove the leader from the targets & the replicas
	delete(targets, vnic.Resources().SysConfig().LocalUuid)
	delete(replicas, vnic.Resources().SysConfig().LocalUuid)

	return isLeader, isLeaderATarget, targets, replicas
}
