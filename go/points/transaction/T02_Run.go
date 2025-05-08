package transaction

import (
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/servicepoints/go/points/transaction/requests"
	"github.com/saichler/l8types/go/ifs"
	"sync"
)

func (this *ServiceTransactions) run(msg ifs.IMessage, vnic ifs.IVNic, cond *sync.Cond) ifs.ITransaction {
	isLeader, isLeaderATarget, targets, replicas := Targets(msg, vnic)
	cond.L.Lock()
	defer func() {
		defer cond.L.Unlock()
		vnic.Resources().Logger().Debug("Tr Leader Cleanup")
		//Cleanup
		oldState := msg.Tr().State()
		msg.Tr().SetState(ifs.Finish)
		requests.RequestFromPeers(msg, vnic, targets)
		this.finish(msg)
		msg.Tr().SetState(oldState)
		cond.Broadcast()
		vnic.Resources().Logger().Debug("Tr Leader Cleanup finished")
	}()

	//If the state isn't Start, this means there is a major bug so panic
	if msg.Tr().State() != ifs.Start {
		panic("start: Unexpected transaction state " + msg.Tr().State().String())
	}

	//There is a race condition, if the leader has changed during this transaction
	//Fail it
	if !isLeader {
		msg.Tr().SetState(ifs.Errored)
		msg.Tr().SetErrorMessage("Start transaction invoked on a follower")
		return msg.Tr()
	}

	vnic.Resources().Logger().Debug("Tr Leader Lock followers")

	//Try to lock on all the followers
	msg.Tr().SetState(ifs.Lock)
	ok, _ := requests.RequestFromPeers(msg, vnic, targets)
	if !ok {
		msg.Tr().SetState(ifs.Errored)
		msg.Tr().SetErrorMessage("Failed to lock followers")
		return msg.Tr()
	}

	vnic.Resources().Logger().Debug("Tr Leader Lock leader")

	//now try to lock on the leader
	msg.Tr().SetState(ifs.Lock)
	ok = this.lock(msg)
	//We were not able to lock on the leader
	if !ok {
		msg.Tr().SetState(ifs.Errored)
		msg.Tr().SetErrorMessage("Failed to lock leader")
		return msg.Tr()
	}

	vnic.Resources().Logger().Debug("Tr Leader Commit followers")

	//At this point we are ready to commit
	//Try to commit on the followers
	//Note we do it on the replicas and not on targets as if this is a replication
	//count commit, we want to commit only on the replicas
	msg.Tr().SetState(ifs.Commit)
	ok, peers := requests.RequestFromPeers(msg, vnic, replicas)
	if !ok {
		//Request a rollback only from those peers that commited
		msg.Tr().SetState(ifs.Rollback)
		rollTarget := make(map[string]bool)
		for target, e := range peers {
			if e == "" {
				rollTarget[target] = true
			}
		}
		requests.RequestFromPeers(msg, vnic, rollTarget)

		msg.Tr().SetState(ifs.Errored)
		msg.Tr().SetErrorMessage("Followers failed to commit")
		return msg.Tr()
	}

	vnic.Resources().Logger().Debug("Tr Leader Commit leader")

	//Try to commit on the leader, if you need to
	if isLeaderATarget {
		msg.Tr().SetState(ifs.Commit)
		ok = this.commit(msg, vnic)
		if !ok {
			//Request a rollback from the followers
			msg.Tr().SetState(ifs.Rollback)
			requests.RequestFromPeers(msg, vnic, replicas)

			errorMsg := "Leader failed to commit"
			if !ok {
				errorMsg = "Leader failed to commit and failed to clean up"
			}
			msg.Tr().SetState(ifs.Errored)
			msg.Tr().SetErrorMessage(errorMsg)
			return msg.Tr()
		}
	}

	vnic.Resources().Logger().Debug("Tr Leader Commited")

	//Cleanup and release the lock
	msg.Tr().SetState(ifs.Commited)
	return msg.Tr()
}

func Targets(msg ifs.IMessage, vnic ifs.IVNic) (bool, bool, map[string]bool, map[string]bool) {
	healthCenter := health.Health(vnic.Resources())
	isLeader := healthCenter.Leader(msg.ServiceName(), msg.ServiceArea()) == vnic.Resources().SysConfig().LocalUuid
	targets := healthCenter.Uuids(msg.ServiceName(), msg.ServiceArea())
	replicas := make(map[string]bool)
	for target, _ := range targets {
		replicas[target] = true
	}
	isLeaderATarget := true

	isReplication, leaderATarget, reps := replicationTargets(vnic, msg)
	if isReplication {
		isLeaderATarget = leaderATarget
		replicas = reps
	}

	//Remove the leader from the targets & the replicas
	delete(targets, vnic.Resources().SysConfig().LocalUuid)
	delete(replicas, vnic.Resources().SysConfig().LocalUuid)

	return isLeader, isLeaderATarget, targets, replicas
}
