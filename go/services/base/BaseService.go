package base

import (
	"reflect"

	"github.com/saichler/l8reflect/go/reflect/helping"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8web"
	"github.com/saichler/l8utils/go/utils/cache"
)

type BaseService struct {
	cache         *cache.Cache
	vnic          ifs.IVNic
	serviceConfig *ifs.ServiceConfig
}

func (this *BaseService) Post(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	createNotification := this.serviceConfig.SendNotifications && !pb.Notification()
	if this.vnic != nil {
		vnic = this.vnic
	}
	for _, elem := range pb.Elements() {
		n, e := this.cache.Post(elem, createNotification)
		if createNotification && e == nil && n != nil {
			go vnic.PropertyChangeNotification(n)
		}
	}
	return object.New(nil, &l8web.L8Empty{})
}

func (this *BaseService) Put(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	createNotification := this.serviceConfig.SendNotifications && !pb.Notification()
	if this.vnic != nil {
		vnic = this.vnic
	}
	for _, elem := range pb.Elements() {
		n, e := this.cache.Put(elem, createNotification)
		if createNotification && e == nil && n != nil {
			go vnic.PropertyChangeNotification(n)
		}
	}
	return object.New(nil, &l8web.L8Empty{})
}

func (this *BaseService) Patch(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	createNotification := this.serviceConfig.SendNotifications && !pb.Notification()
	if this.vnic != nil {
		vnic = this.vnic
	}
	for _, elem := range pb.Elements() {
		n, e := this.cache.Patch(elem, createNotification)
		if createNotification && e == nil && n != nil {
			go vnic.PropertyChangeNotification(n)
		}
	}
	return object.New(nil, &l8web.L8Empty{})
}

func (this *BaseService) Delete(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	createNotification := this.serviceConfig.SendNotifications && !pb.Notification()
	if this.vnic != nil {
		vnic = this.vnic
	}
	for _, elem := range pb.Elements() {
		n, e := this.cache.Delete(elem, createNotification)
		if createNotification && e == nil && n != nil {
			go vnic.PropertyChangeNotification(n)
		}
	}
	return object.New(nil, &l8web.L8Empty{})
}

func (this *BaseService) Get(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	if pb.IsFilterMode() {
		e := this.validateElem(pb)
		if e != nil {
			return object.New(e, &l8web.L8Empty{})
		}
		resp, err := this.cache.Get(pb.Element())
		return object.New(err, resp)
	}
	q, e := pb.Query(this.vnic.Resources())
	if e != nil {
		return object.NewError(e.Error())
	}
	elems := this.cache.Fetch(int(q.Page()*q.Limit()), int(q.Limit()), q)
	return object.New(nil, elems)
}

func (this *BaseService) Failed(pb ifs.IElements, vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	this.vnic.Resources().Logger().Error("Failed to deliver message")
	return nil
}

func (this *BaseService) TransactionConfig() ifs.ITransactionConfig {
	if this.serviceConfig.Transaction {
		if this.serviceConfig.SendNotifications {
			this.vnic.Resources().Logger().Warning("Both notification and transaction were enabled, diabling notifications ")
			this.serviceConfig.SendNotifications = false
		}
		return this
	}
	return nil
}

func (this *BaseService) WebService() ifs.IWebService {
	return this.serviceConfig.WebServiceDef
}

func (this *BaseService) Replication() bool {
	return this.serviceConfig.Replication
}

func (this *BaseService) ReplicationCount() int {
	return this.serviceConfig.ReplicationCount
}

func (this *BaseService) KeyOf(elems ifs.IElements, r ifs.IResources) string {
	node, _ := r.Introspector().Node(this.cache.ModelType())
	key := helping.PrimaryDecorator(node, reflect.ValueOf(elems.Element()), this.vnic.Resources().Registry())
	return key.(string)
}

func (this *BaseService) ConcurrentGets() bool {
	return false
}
