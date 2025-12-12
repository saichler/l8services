package manager

import (
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceManager) handle(h ifs.IServiceHandler, pb ifs.IElements, action ifs.Action, msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	if h == nil {
		return object.New(nil, pb)
	}
	_, isMapReduce := h.(ifs.IMapReduceService)
	switch action {
	case ifs.POST:
		return h.Post(pb, vnic)
	case ifs.PUT:
		return h.Put(pb, vnic)
	case ifs.PATCH:
		return h.Patch(pb, vnic)
	case ifs.DELETE:
		return h.Delete(pb, vnic)
	case ifs.GET:
		return h.Get(pb, vnic)
	default:
		if isMapReduce {
			return this.MapReduce(h, pb, action, msg, vnic)
		}
		return object.NewError("invalid action, ignoring")
	}
}
