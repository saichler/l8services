package manager

import (
	"strconv"
	"sync"

	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceManager) MapReduce(h ifs.IServiceHandler, pb ifs.IElements, action ifs.Action, msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	cMsg := msg.Clone()
	switch action {
	case ifs.MapR_POST:
		cMsg.SetAction(ifs.POST)
	case ifs.MapR_PUT:
		cMsg.SetAction(ifs.PUT)
	case ifs.MapR_DELETE:
		cMsg.SetAction(ifs.DELETE)
	case ifs.MapR_PATCH:
		cMsg.SetAction(ifs.PATCH)
	case ifs.MapR_GET:
		cMsg.SetAction(ifs.GET)
	default:
		panic("unknown action " + strconv.Itoa(int(action)))
	}
	results := this.PeerRequest(cMsg, vnic)
	mh := h.(ifs.IMapReduceService)
	return mh.Merge(results)
}

func (this *ServiceManager) PeerRequest(msg *ifs.Message, nic ifs.IVNic) map[string]ifs.IElements {
	edges := this.GetParticipants(msg.ServiceName(), msg.ServiceArea())
	wg := sync.WaitGroup{}
	mtx := sync.Mutex{}
	results := make(map[string]ifs.IElements)
	for uuid, _ := range edges {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := nic.Forward(msg, uuid)
			mtx.Lock()
			results[uuid] = resp
			mtx.Unlock()
		}()
	}
	wg.Wait()
	return results
}
