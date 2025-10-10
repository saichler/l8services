package requests

import (
	"sync"

	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8services"
)

type Requests struct {
	cond    *sync.Cond
	pending map[string]string
	count   int
	vnic    ifs.IVNic
}

func NewRequest(vnic ifs.IVNic) *Requests {
	rq := &Requests{}
	rq.pending = make(map[string]string)
	rq.cond = sync.NewCond(&sync.Mutex{})
	rq.vnic = vnic
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

func (this *Requests) reportResult(target string, tr *l8services.L8Transaction) {
	this.cond.L.Lock()
	defer this.cond.L.Unlock()
	if tr.State == int32(ifs.Failed) {
		this.pending[target] = tr.ErrMsg
	}
	this.count--
	if this.count == 0 {
		this.cond.Broadcast()
	}
}

func (this *Requests) requestFromPeer(msg *ifs.Message, target string, isReplicate bool, replicateNum byte) {
	this.addOne(target)

	if isReplicate {
		clone := msg.Clone()
		clone.SetTr_Replica(replicateNum)
		msg = clone
	}

	resp := this.vnic.Forward(msg, target)
	if resp != nil && resp.Error() != nil {
		this.vnic.Resources().Logger().Error(resp.Error())
		this.reportError(target, resp.Error())
		return
	}

	tr := resp.Element().(*l8services.L8Transaction)
	this.reportResult(target, tr)
}

func RequestFromPeers(msg *ifs.Message, targets map[string]byte, vnic ifs.IVNic, isReplicate bool) (bool, map[string]string) {

	this := NewRequest(vnic)

	this.cond.L.Lock()
	defer this.cond.L.Unlock()

	for target, replica := range targets {
		go this.requestFromPeer(msg, target, isReplicate, replica)
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
		msg.SetTr_State(ifs.Failed)
		return false, this.pending
	}

	return true, this.pending
}

/*
func IsLeader(resourcs ifs.IResources, localUuid, topic string, serviceArea byte) (bool, string) {
	hc := health.Health(resourcs)
	leader := hc.LeaderFor(topic, serviceArea)
	return leader == localUuid, leader
}*/
