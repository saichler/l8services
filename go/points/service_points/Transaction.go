package service_points

import (
	"errors"
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
	"google.golang.org/protobuf/proto"
)

func (this *ServicePointsImpl) runTransaction(h interfaces.IServicePointHandler, pb proto.Message, msg *types.Message,
	vnic interfaces.IVirtualNetworkInterface) (proto.Message, error) {
	hc := health.Health(vnic.Resources())
	leader := hc.Leader(msg.Type, msg.Vlan)
	uuid := vnic.Resources().Config().LocalUuid
	isLeader := leader == uuid

	if msg.Tr == nil {
		ok := this.createTransaction(msg)
		if !ok {
			return msg.Tr, errors.New("transaction: Cannot start while another is running")
		}

		ok = this.requestLock(msg, vnic)
		if !ok {
			return msg.Tr, errors.New("transaction: Failed to aquire lock")
		}

		msg.Tr.State = types.TransactionState_Commit

		if isLeader {
			r, err := vnic.Forward(msg, leader)
			resp, _ := r.(proto.Message)
			return resp, err
		}
	}

	if msg.Tr.State == types.TransactionState_Commit {
		if isLeader {
			ok := this.requestCommit(msg, vnic)
			if !ok {
				//@TODO request rollback
				this.requestFinish(msg, vnic)

				this.trCond.L.Lock()
				defer this.trCond.L.Unlock()
				msg.Tr.State = types.TransactionState_Finished
				delete(this.transactions, msg.Type)

				return msg.Tr, errors.New("transaction: Failed to commit")
			}
		}
		resp, err := this.doAction(h, msg.Action, pb, vnic.Resources())
		if msg != nil && msg.Tr != nil {
			if err != nil {
				msg.Tr.State = types.TransactionState_Rollback
			} else {
				msg.Tr.State = types.TransactionState_Commited
			}
		}
		if !isLeader {
			return resp, err
		} else {
			ok := this.requestFinish(msg, vnic)
			if !ok {
				//@TODO request rollback???
				return msg.Tr, errors.New("transaction: Failed to finish")
			}
		}
		this.trCond.L.Lock()
		defer this.trCond.L.Unlock()
		msg.Tr.State = types.TransactionState_Finished
		delete(this.transactions, msg.Type)
		return msg.Tr, nil
	}

	if msg.Tr.State == types.TransactionState_Lock {
		this.trCond.L.Lock()
		defer this.trCond.L.Unlock()
		if this.transactions[msg.Type] == nil {
			msg.Tr.State = types.TransactionState_Locked
			this.transactions[msg.Type] = msg
			return msg.Tr, nil
		} else {
			msg.Tr.State = types.TransactionState_Rollback
			return msg.Tr, nil
		}
	} else if msg.Tr.State == types.TransactionState_Rollback {
		this.trCond.L.Lock()
		defer this.trCond.L.Unlock()
		//@TODO Rollback msg
		msg.Tr.State = types.TransactionState_Rollbacked
		return msg.Tr, nil
	} else if msg.Tr.State == types.TransactionState_Finish {
		this.trCond.L.Lock()
		defer this.trCond.L.Unlock()
		msg.Tr.State = types.TransactionState_Finished
		delete(this.transactions, msg.Type)
		return msg.Tr, nil
	}
	panic("")
}

func (this *ServicePointsImpl) createTransaction(msg *types.Message) bool {
	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()

	msg.Tr = &types.Transaction{}
	msg.Tr.Id = interfaces.NewUuid()
	msg.Tr.State = types.TransactionState_Lock

	_, ok := this.transactions[msg.Type]
	if ok {
		msg.Tr.State = types.TransactionState_Rollback
		return false
	}
	this.trState = make(map[string]bool)
	this.transactions[msg.Type] = msg
	return true
}

func (this *ServicePointsImpl) requestLock(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) bool {
	//Lock state coming from the create method
	hc := health.Health(vnic.Resources())
	targets := hc.Uuids(msg.Type, msg.Vlan, true)
	delete(targets, vnic.Resources().Config().LocalUuid)
	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()
	for target, _ := range targets {
		go this.request(vnic, msg, target)
	}
	this.trCond.Wait()
	if len(this.trState) != 0 {
		msg.Tr.State = types.TransactionState_Rollback
		for target, _ := range targets {
			go vnic.Forward(msg, target)
		}
		delete(this.transactions, msg.Type)
		return false
	}
	return true
}

func (this *ServicePointsImpl) requestCommit(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) bool {
	msg.Tr.State = types.TransactionState_Commit
	hc := health.Health(vnic.Resources())
	targets := hc.Uuids(msg.Type, msg.Vlan, true)
	delete(targets, vnic.Resources().Config().LocalUuid)
	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()
	for target, _ := range targets {
		go this.request(vnic, msg, target)
	}
	this.trCond.Wait()
	if len(this.trState) != 0 {
		msg.Tr.State = types.TransactionState_Rollback
		for target, _ := range targets {
			go vnic.Forward(msg, target)
		}
		delete(this.transactions, msg.Type)
		return false
	}
	return true
}

func (this *ServicePointsImpl) requestFinish(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) bool {
	msg.Tr.State = types.TransactionState_Finish
	hc := health.Health(vnic.Resources())
	targets := hc.Uuids(msg.Type, msg.Vlan, true)
	delete(targets, vnic.Resources().Config().LocalUuid)
	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()
	for target, _ := range targets {
		go this.request(vnic, msg, target)
	}
	this.trCond.Wait()
	if len(this.trState) != 0 {
		msg.Tr.State = types.TransactionState_Rollback
		for target, _ := range targets {
			go vnic.Forward(msg, target)
		}
		delete(this.transactions, msg.Type)
		return false
	}
	return true
}

func (this *ServicePointsImpl) request(vnic interfaces.IVirtualNetworkInterface, msg *types.Message, target string) {
	this.trCond.L.Lock()
	this.trState[target] = true
	this.trCond.L.Unlock()
	resp, err := vnic.Forward(msg, target)
	if err != nil {
		this.trCond.L.Lock()
		defer this.trCond.L.Unlock()
		this.trState[target] = false
		this.trCond.Broadcast()
		return
	}

	tr := resp.(*types.Transaction)
	if tr.State == types.TransactionState_Rollback {
		this.trCond.L.Lock()
		defer this.trCond.L.Unlock()
		this.trState[target] = false
		this.trCond.Broadcast()
		return
	}
	this.trCond.L.Lock()
	defer this.trCond.L.Unlock()
	delete(this.trState, target)
	if len(this.trState) == 0 {
		this.trCond.Broadcast()
	}
}
