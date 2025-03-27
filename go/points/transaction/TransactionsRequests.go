package transaction

import (
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"sync"
)

type Requests struct {
	cond    *sync.Cond
	pending map[string]string
	count   int
}

func newRequest() *Requests {
	rq := &Requests{}
	rq.pending = make(map[string]string)
	rq.cond = sync.NewCond(&sync.Mutex{})
	return rq
}

func (this *Requests) requestFromPeer(vnic common.IVirtualNetworkInterface, msg *types.Message, target string) {
	this.cond.L.Lock()
	this.pending[target] = ""
	this.count++
	this.cond.L.Unlock()

	resp := vnic.Forward(msg, target)
	if resp.Err() != nil {
		this.cond.L.Lock()
		defer this.cond.L.Unlock()
		this.pending[target] = resp.Err().Error()
		this.cond.Broadcast()
		return
	}

	tr := resp.Elem().(*types.Transaction)

	this.cond.L.Lock()
	defer this.cond.L.Unlock()

	if tr.State == types.TransactionState_Errored {
		this.pending[target] = tr.Error
	}

	this.count--

	if this.count == 0 {
		this.cond.Broadcast()
	}
}

func requestFromPeers(msg *types.Message, vnic common.IVirtualNetworkInterface, targets map[string]bool) (bool, map[string]string) {

	this := newRequest()

	this.cond.L.Lock()
	defer this.cond.L.Unlock()

	for target, _ := range targets {
		go this.requestFromPeer(vnic, msg, target)
	}

	//@TODO - implement timeout
	this.cond.Wait()

	ok := true
	for _, e := range this.pending {
		if e != "" {
			ok = false
			break
		}
	}

	if !ok {
		msg.Tr.State = types.TransactionState_Errored
		return false, this.pending
	}

	return true, this.pending
}

func IsLeader(resourcs common.IResources, localUuid, topic string, vlan int32) (bool, string) {
	hc := health.Health(resourcs)
	leader := hc.Leader(topic, vlan)
	return leader == localUuid, leader
}
