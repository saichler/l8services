package base

import (
	"fmt"

	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8notify"
	"github.com/saichler/l8types/go/types/l8web"
)

func (this *BaseService) do(action ifs.Action, pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	createNotification := this.sla.Stateful() && this.sla.Voter() && !pb.Notification()
	if this.vnic != nil {
		vnic = this.vnic
	}
	for _, elem := range pb.Elements() {
		if elem == nil {
			continue
		}
		var n *l8notify.L8NotificationSet
		var e error
		switch action {
		case ifs.POST:
			if this.sla.Callback() != nil {
				beforElem := this.sla.Callback().BeforePost(elem, vnic)
				if beforElem != nil {
					elem = beforElem
				}
			}
			n, e = this.cache.Post(elem, createNotification)
			if this.sla.Callback() != nil {
				this.sla.Callback().AfterPost(elem, vnic)
			}
		case ifs.PUT:
			if this.sla.Callback() != nil {
				beforElem := this.sla.Callback().BeforePut(elem, vnic)
				if beforElem != nil {
					elem = beforElem
				}
			}
			n, e = this.cache.Put(elem, createNotification)
			if this.sla.Callback() != nil {
				this.sla.Callback().AfterPut(elem, vnic)
			}
		case ifs.PATCH:
			if this.sla.Callback() != nil {
				beforElem := this.sla.Callback().BeforePatch(elem, vnic)
				if beforElem != nil {
					elem = beforElem
				}
			}
			n, e = this.cache.Patch(elem, createNotification)
			if this.sla.Callback() != nil {
				this.sla.Callback().AfterPatch(elem, vnic)
			}
		case ifs.DELETE:
			if this.sla.Callback() != nil {
				beforElem := this.sla.Callback().BeforeDelete(elem, vnic)
				if beforElem != nil {
					elem = beforElem
				}
			}
			n, e = this.cache.Delete(elem, createNotification)
			if this.sla.Callback() != nil {
				this.sla.Callback().AfterDelete(elem, vnic)
			}
		}
		if e != nil {
			fmt.Println("Error in notification: ", e.Error())
		}
		if createNotification && e == nil && n != nil {
			this.nQueue.Add(n)
		}
	}
	return object.New(nil, &l8web.L8Empty{})
}

func (this *BaseService) processNotificationQueue() {
	for this.running {
		set, ok := this.nQueue.Next().(*l8notify.L8NotificationSet)
		if ok {
			this.vnic.PropertyChangeNotification(set)
		}
	}
}

func (this *BaseService) Shutdown() {
	this.running = false
	this.nQueue.Add(nil)
}
