package transaction

import (
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/types/go/common"
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

func (this *Requests) requestFromPeer(vnic common.IVirtualNetworkInterface, msg common.IMessage, target string) {
	this.cond.L.Lock()
	this.pending[target] = ""
	this.count++
	this.cond.L.Unlock()

	resp := vnic.Forward(msg, target)
	if resp != nil && resp.Error() != nil {
		this.cond.L.Lock()
		defer this.cond.L.Unlock()
		this.pending[target] = resp.Error().Error()
		this.cond.Broadcast()
		return
	}

	tr := resp.Element().(common.ITransaction)

	this.cond.L.Lock()
	defer this.cond.L.Unlock()

	if tr.State() == common.Errored {
		this.pending[target] = tr.ErrorMessage()
	}

	this.count--

	if this.count == 0 {
		this.cond.Broadcast()
	}
}

func requestFromPeers(msg common.IMessage, vnic common.IVirtualNetworkInterface, targets map[string]bool) (bool, map[string]string) {

	this := newRequest()

	this.cond.L.Lock()
	defer this.cond.L.Unlock()

	for target, _ := range targets {
		go this.requestFromPeer(vnic, msg, target)
	}

	//@TODO - implement timeout
	if len(targets) > 0 {
		this.cond.Wait()
	}

	ok := true
	for _, e := range this.pending {
		if e != "" {
			ok = false
			break
		}
	}

	if !ok {
		msg.Tr().SetState(common.Errored)
		return false, this.pending
	}

	return true, this.pending
}

func IsLeader(resourcs common.IResources, localUuid, topic string, serviceArea uint16) (bool, string) {
	hc := health.Health(resourcs)
	leader := hc.Leader(topic, serviceArea)
	return leader == localUuid, leader
}
