package service_points

import (
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
)

func (this *Transactions) requestFromPeer(vnic interfaces.IVirtualNetworkInterface, msg *types.Message, target string) {
	this.cond.L.Lock()
	this.pendingPeerRequests[target] = true
	this.cond.L.Unlock()

	resp, err := vnic.Forward(msg, target)
	if err != nil {
		this.cond.L.Lock()
		defer this.cond.L.Unlock()
		this.pendingPeerRequests[target] = false
		this.cond.Broadcast()
		return
	}

	tr := resp.(*types.Tr)

	this.cond.L.Lock()
	defer this.cond.L.Unlock()

	if tr.State == types.TrState_Errored {
		this.pendingPeerRequests[target] = false
		this.cond.Broadcast()
		return
	}

	delete(this.pendingPeerRequests, target)
	if len(this.pendingPeerRequests) == 0 {
		this.cond.Broadcast()
	}
}

func (this *Transactions) requestFromAllPeers(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) bool {
	hc := health.Health(vnic.Resources())
	targets := hc.Uuids(msg.Type, msg.Vlan, true)
	delete(targets, vnic.Resources().Config().LocalUuid)

	this.cond.L.Lock()
	defer this.cond.L.Unlock()

	if this.pendingPeerRequests == nil {
		this.pendingPeerRequests = make(map[string]bool)
	}

	for target, _ := range targets {
		go this.requestFromPeer(vnic, msg, target)
	}

	//@TODO - implement timeout
	this.cond.Wait()

	if len(this.pendingPeerRequests) != 0 {
		msg.Tr.State = types.TrState_Errored
		for target, _ := range targets {
			go vnic.Forward(msg, target)
		}
		this.pendingPeerRequests = make(map[string]bool)
		return false
	}

	return true
}

func IsLeader(resourcs interfaces.IResources, localUuid, topic string, vlan int32) (bool, string) {
	hc := health.Health(resourcs)
	leader := hc.Leader(topic, vlan)
	return leader == localUuid, leader
}
