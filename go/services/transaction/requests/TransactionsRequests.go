package requests

import (
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types"
	"github.com/saichler/layer8/go/overlay/health"
	"sync"
)

type Requests struct {
	cond    *sync.Cond
	pending map[string]string
	count   int
}

func NewRequest() *Requests {
	rq := &Requests{}
	rq.pending = make(map[string]string)
	rq.cond = sync.NewCond(&sync.Mutex{})
	return rq
}

func (this *Requests) addOne(target string) {
	this.cond.L.Lock()
	defer this.cond.L.Unlock()
	this.pending[target] = ""
	this.count++
}

func (this *Requests) reportError(target string, err error) {
	this.cond.L.Lock()
	defer this.cond.L.Unlock()
	this.pending[target] = err.Error()
	this.count--
	if this.count == 0 {
		this.cond.Broadcast()
	}
}

func (this *Requests) reportResult(target string, tr *types.Transaction) {
	this.cond.L.Lock()
	defer this.cond.L.Unlock()
	if tr.State == int32(ifs.Errored) {
		this.pending[target] = tr.ErrMsg
	}
	this.count--
	if this.count == 0 {
		this.cond.Broadcast()
	}
}

func (this *Requests) requestFromPeer(vnic ifs.IVNic, msg *ifs.Message, target string) {
	this.addOne(target)

	resp := vnic.Forward(msg, target)
	if resp != nil && resp.Error() != nil {
		this.reportError(target, resp.Error())
		return
	}

	tr := resp.Element().(*types.Transaction)
	this.reportResult(target, tr)
}

func RequestFromPeers(msg *ifs.Message, vnic ifs.IVNic, targets map[string]bool) (bool, map[string]string) {

	this := NewRequest()

	this.cond.L.Lock()
	defer this.cond.L.Unlock()

	for target, _ := range targets {
		go this.requestFromPeer(vnic, msg, target)
	}

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
		msg.SetTr_State(ifs.Errored)
		return false, this.pending
	}

	return true, this.pending
}

func IsLeader(resourcs ifs.IResources, localUuid, topic string, serviceArea byte) (bool, string) {
	hc := health.Health(resourcs)
	leader := hc.LeaderFor(topic, serviceArea)
	return leader == localUuid, leader
}
