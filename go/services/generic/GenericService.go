package generic

import (
	"reflect"

	"github.com/saichler/l8reflect/go/reflect/helping"
	"github.com/saichler/l8reflect/go/reflect/introspecting"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8web"
	"github.com/saichler/l8utils/go/utils/cache"
)

type GenericService struct {
	cache         *cache.Cache
	vnic          ifs.IVNic
	serviceConfig *ifs.ServiceConfig
}

func Activate(serviceConfig *ifs.ServiceConfig, vnic ifs.IVNic) error {
	vnic.Resources().Registry().Register(&GenericService{})
	vnic.Resources().Registry().Register(serviceConfig.ServiceItemList)
	vnic.Resources().Registry().Register(&l8web.L8Empty{})
	node, _ := vnic.Resources().Introspector().Inspect(serviceConfig.ServiceItem)
	introspecting.AddPrimaryKeyDecorator(node, serviceConfig.PrimaryKey...)
	_, e := vnic.Resources().Services().Activate("GenericService", serviceConfig.ServiceName, serviceConfig.ServiceArea,
		vnic.Resources(), vnic, serviceConfig)
	return e
}

func (this *GenericService) Activate(serviceName string, serviceArea byte,
	resources ifs.IResources, listener ifs.IServiceCacheListener, args ...interface{}) error {
	this.serviceConfig = args[0].(*ifs.ServiceConfig)
	this.cache = cache.NewCache(this.serviceConfig.ServiceItem, this.serviceConfig.InitItems,
		this.serviceConfig.Store, resources)
	this.cache.SetNotificationsFor(serviceName, serviceArea)
	this.vnic = listener.(ifs.IVNic)
	if this.serviceConfig.SendNotifications {
		//resources.Services().RegisterServiceCache(this)
	}
	return nil
}

func (this *GenericService) DeActivate() error {
	return nil
}

func (this *GenericService) Post(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
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

func (this *GenericService) Put(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
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

func (this *GenericService) Patch(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
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

func (this *GenericService) Delete(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
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

func (this *GenericService) Get(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	if pb.IsFilterMode() {
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

func (this *GenericService) Failed(pb ifs.IElements, vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	this.vnic.Resources().Logger().Error("Failed to deliver message")
	return nil
}

func (this *GenericService) TransactionConfig() ifs.ITransactionConfig {
	if this.serviceConfig.Transaction {
		if this.serviceConfig.SendNotifications {
			this.vnic.Resources().Logger().Warning("Both notification and transaction were enabled, diabling notifications ")
			this.serviceConfig.SendNotifications = false
		}
		return this
	}
	return nil
}

func (this *GenericService) WebService() ifs.IWebService {
	return this.serviceConfig.WebServiceDef
}

func (this *GenericService) Replication() bool {
	return this.serviceConfig.Replication
}

func (this *GenericService) ReplicationCount() int {
	return this.serviceConfig.ReplicationCount
}

func (this *GenericService) KeyOf(elems ifs.IElements, r ifs.IResources) string {
	node, _ := r.Introspector().Node(this.cache.ModelType())
	key := helping.PrimaryDecorator(node, reflect.ValueOf(elems.Element()), this.vnic.Resources().Registry())
	return key.(string)
}

func (this *GenericService) ConcurrentGets() bool {
	return false
}
