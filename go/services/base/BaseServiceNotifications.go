package base

import (
	"fmt"

	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8notify"
	"github.com/saichler/l8types/go/types/l8web"
)

func (this *BaseService) do(action ifs.Action, pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	if pb.Notification() {
		fmt.Println("Received notification")
	}
	createNotification := this.sla.Stateful() && this.sla.Voter() && !pb.Notification()
	if this.vnic != nil {
		vnic = this.vnic
	}
	for _, elem := range pb.Elements() {
		var n *l8notify.L8NotificationSet
		var e error
		switch action {
		case ifs.POST:
			n, e = this.cache.Post(elem, createNotification)
		case ifs.PUT:
			n, e = this.cache.Put(elem, createNotification)
		case ifs.PATCH:
			n, e = this.cache.Patch(elem, createNotification)
		case ifs.DELETE:
			n, e = this.cache.Delete(elem, createNotification)
		}
		if e != nil {
			fmt.Println("Error in notification: ", e.Error())
		}
		if createNotification && e == nil && n != nil {
			fmt.Println("Adding health notification")
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
