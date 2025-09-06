package states

import (
	"github.com/saichler/l8services/go/services/transaction"
	"github.com/saichler/l8services/go/services/transaction/requests"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types"
)

func (this *ServiceTransactions) run(tr *transaction.Transaction) *types.Transaction {
	tr.Debug("T02_Run.run: ", tr.Msg().Tr_Id())
	isLeader, isLeaderATarget, targets, replicas := tr.TargetsWithReplication()
	this.mtx.Lock()
	defer func() {
		defer this.cond.Broadcast()
		defer this.mtx.Unlock()
		tr.Debug("T02_Run.run: Transaction leader cleanup ", tr.Msg().Tr_Id())
		//Cleanup
		oldState := tr.Msg().Tr_State()
		tr.Msg().SetTr_State(ifs.Finish)
		requests.RequestFromPeers(tr, targets)
		this.finishInternal(tr.Msg())
		tr.Msg().SetTr_State(oldState)
		tr.Debug("T02_Run.run: Transaction leader cleanup finished ", tr.Msg().Tr_Id())
		tr.Cond().Broadcast()
	}()

	//If the state isn't Start, this means there is a major bug so panic
	if tr.Msg().Tr_State() != ifs.Start {
		panic("T02_Run.run: Unexpected transaction state " + tr.Msg().Tr_State().String() + " " + tr.Msg().Tr_Id())
	}

	//There is a race condition, if the leader has changed during this transaction
	//Fail it
	if !isLeader {
		tr.Msg().SetTr_State(ifs.Errored)
		tr.Msg().SetTr_ErrMsg("T02_Run.run: Start transaction invoked on a follower " + tr.Msg().Tr_Id())
		tr.Error(tr.Msg().Tr_ErrMsg())
		return TransactionOf(tr)
	}

	tr.Debug("T02_Run.run: Transaction leader locking followers ", tr.Msg().Tr_Id())
	//Try to lock on all the followers
	tr.Msg().SetTr_State(ifs.Created)
	ok, _ := requests.RequestFromPeers(tr, targets)
	if !ok {
		tr.Msg().SetTr_State(ifs.Errored)
		tr.Msg().SetTr_ErrMsg("T02_Run.run: Transaction leader failed to lock followers " + tr.Msg().Tr_Id())
		tr.Error(tr.Msg().Tr_ErrMsg())
		return TransactionOf(tr)
	}

	tr.Debug("T02_Run.run: Transaction leader lock on leader ", tr.Msg().Tr_Id())

	//now try to lock on the leader
	state, _ := this.lockInternal(tr)
	//We were not able to lock on the leader
	if state == ifs.Errored {
		return TransactionOf(tr)
	}

	tr.Debug("T02_Run.run: Transaction commit on followers ", tr.Msg().Tr_Id())
	//At this point we are ready to commit
	//Try to commit on the followers
	//Note we do it on the replicas and not on targets as if this is a replication
	//count commit, we want to commit only on the replicas
	tr.Msg().SetTr_State(ifs.Commit)
	ok, peers := requests.RequestFromPeers(tr, replicas)
	if !ok {
		//Request a rollback only from those peers that commited
		tr.Msg().SetTr_State(ifs.Rollback)
		rollTarget := make(map[string]bool)
		for target, e := range peers {
			if e == "" {
				rollTarget[target] = true
			}
		}
		requests.RequestFromPeers(tr, rollTarget)
		tr.Msg().SetTr_State(ifs.Errored)
		tr.Msg().SetTr_ErrMsg("T02_Run.run: Failed to commit on Followers " + tr.Msg().Tr_Id())
		tr.Error(tr.Msg().Tr_ErrMsg())
		return TransactionOf(tr)
	}

	//Try to commit on the leader, if you need to
	if isLeaderATarget {
		tr.Debug("T02_Run.run: Transaction commit on leader ", tr.Msg().Tr_Id())
		tr.Msg().SetTr_State(ifs.Commit)
		state, _ = this.commitInternal(tr)
		if state == ifs.Errored {
			err := tr.Error("T02_Run.run: Transaction leader failed to commit ", tr.Msg().Tr_Id())
			//Request a rollback from the followers
			tr.Msg().SetTr_State(ifs.Rollback)
			ok, _ = requests.RequestFromPeers(tr, replicas)
			if !ok {
				err = tr.Error("T02_Run.run: Transaction failed to rollback on followers after failed commit on leader ", tr.Msg().Tr_Id())
			}
			tr.Msg().SetTr_State(ifs.Errored)
			tr.Msg().SetTr_ErrMsg(err.Error())
			return TransactionOf(tr)
		}
	}

	tr.Debug("T02_Run.run: Transaction commited ", tr.Msg().Tr_Id())

	//Cleanup and release the lock
	tr.Msg().SetTr_State(ifs.Commited)
	return TransactionOf(tr)
}
