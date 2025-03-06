package transaction

import (
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
	"sync"
)

type Requests struct {
	cond    *sync.Cond
	pending map[string]bool
	count   int
}

func newRequest() *Requests {
	rq := &Requests{}
	rq.pending = make(map[string]bool)
	rq.cond = sync.NewCond(&sync.Mutex{})
	return rq
}

func (this *Requests) requestFromPeer(vnic interfaces.IVirtualNetworkInterface, msg *types.Message, target string) {
	this.cond.L.Lock()
	this.pending[target] = true
	this.count++
	this.cond.L.Unlock()

	resp, err := vnic.Forward(msg, target)
	if err != nil {
		this.cond.L.Lock()
		defer this.cond.L.Unlock()
		this.pending[target] = false
		this.cond.Broadcast()
		return
	}

	tr := resp.(*types.Transaction)

	this.cond.L.Lock()
	defer this.cond.L.Unlock()

	if tr.State == types.TransactionState_Errored {
		this.pending[target] = false
	}

	this.count--

	if this.count == 0 {
		this.cond.Broadcast()
	}
}

func requestFromAllPeers(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) (bool, map[string]bool) {
	return requestFromPeers(msg, vnic, nil)
}

func requestFromPeers(msg *types.Message, vnic interfaces.IVirtualNetworkInterface, peers map[string]bool) (bool, map[string]bool) {
	hc := health.Health(vnic.Resources())
	targets := hc.Uuids(msg.Type, msg.Vlan, true)
	delete(targets, vnic.Resources().Config().LocalUuid)
	if peers != nil {
		for peer, ok := range peers {
			if !ok {
				delete(targets, peer)
			}
		}
	}

	this := newRequest()

	this.cond.L.Lock()
	defer this.cond.L.Unlock()

	for target, _ := range targets {
		go this.requestFromPeer(vnic, msg, target)
	}

	//@TODO - implement timeout
	this.cond.Wait()

	ok := true
	for _, ok = range this.pending {
		if !ok {
			break
		}
	}

	if !ok {
		msg.Tr.State = types.TransactionState_Errored
		return false, this.pending
	}

	return true, this.pending
}

func IsLeader(resourcs interfaces.IResources, localUuid, topic string, vlan int32) (bool, string) {
	hc := health.Health(resourcs)
	leader := hc.Leader(topic, vlan)
	return leader == localUuid, leader
}
