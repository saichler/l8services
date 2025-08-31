package manager

import (
	"bytes"
	"strconv"

	"github.com/saichler/l8services/go/services/dcache"
	"github.com/saichler/l8services/go/services/transaction/states"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types"
	"github.com/saichler/l8utils/go/utils/maps"
	"github.com/saichler/layer8/go/overlay/health"
)

type ServiceManager struct {
	services          *ServicesMap
	trManager         *states.TransactionManager
	distributedCaches *maps.SyncMap
	resources         ifs.IResources
}

func NewServices(resources ifs.IResources) ifs.IServices {
	sp := &ServiceManager{}
	sp.services = NewServicesMap()
	sp.resources = resources
	sp.trManager = states.NewTransactionManager()
	sp.distributedCaches = maps.NewSyncMap()
	_, err := sp.resources.Registry().Register(&types.NotificationSet{})
	if err != nil {
		panic(err)
	}
	sp.resources.Registry().Register(&types.Transaction{})
	return sp
}

func (this *ServiceManager) RegisterServiceHandlerType(handler ifs.IServiceHandler) {
	this.resources.Registry().Register(handler)
}

func (this *ServiceManager) Handle(pb ifs.IElements, action ifs.Action, vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	if vnic == nil {
		return object.NewError("Handle: vnic cannot be nil")
	}
	if msg == nil {
		return object.NewError("Handle: message cannot be nil")
	}
	err := vnic.Resources().Security().CanDoAction(action, pb, vnic.Resources().SysConfig().LocalUuid, "")
	if err != nil {
		return object.NewError(err.Error())
	}

	if msg.Action() == ifs.Sync {
		key := cacheKey(msg.ServiceName(), msg.ServiceArea())
		cache, ok := this.distributedCaches.Get(key)
		if ok {
			go cache.(ifs.IDistributedCache).Sync()
		}
		return nil
	}

	if msg.Action() == ifs.EndPoints {
		this.sendEndPoints(vnic)
		return nil
	}

	h, ok := this.services.get(msg.ServiceName(), msg.ServiceArea())
	if !ok {
		hp := health.Health(vnic.Resources()).Health(vnic.Resources().SysConfig().LocalUuid)
		alias := "Unknown"
		if hp != nil {
			alias = hp.Alias
		}
		return object.NewError(alias + " - Cannot find active handler for service " + msg.ServiceName() +
			" area " + strconv.Itoa(int(msg.ServiceArea())))
	}

	if msg.FailMessage() != "" {
		return h.Failed(pb, vnic, msg)
	}

	if h.TransactionMethod() != nil {
		if msg.Tr_State() == ifs.Empty {
			vnic.Resources().Logger().Debug("Starting transaction")
			defer vnic.Resources().Logger().Debug("Defer Starting transaction")
			return this.trManager.Create(msg, vnic)
		}
		vnic.Resources().Logger().Debug("Running transaction")
		defer vnic.Resources().Logger().Debug("Defer Running transaction")
		return this.trManager.Run(msg, vnic)
	}

	return this.handle(h, pb, action, vnic)
}

func (this *ServiceManager) TransactionHandle(pb ifs.IElements, action ifs.Action, vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	this.resources.Logger().Info("Transaction Handle:", msg.ServiceName(), ",", msg.ServiceArea(), ",", action)
	h, _ := this.services.get(msg.ServiceName(), msg.ServiceArea())
	if h == nil {
		this.resources.Logger().Info("Transaction Handle: No handler for service "+msg.ServiceName(), "-", msg.ServiceArea())
	}
	return this.handle(h, pb, action, vnic)
}

func (this *ServiceManager) handle(h ifs.IServiceHandler, pb ifs.IElements,
	action ifs.Action, vnic ifs.IVNic) ifs.IElements {

	if h == nil {
		return object.New(nil, pb)
	}

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
		return object.NewError("invalid action, ignoring")
	}
}

func (this *ServiceManager) Notify(pb ifs.IElements, vnic ifs.IVNic, msg *ifs.Message, isTransaction bool) ifs.IElements {
	if vnic.Resources().SysConfig().LocalUuid == msg.Source() {
		return object.New(nil, nil)
	}
	notification := pb.Element().(*types.NotificationSet)
	h, ok := this.services.get(notification.ServiceName, byte(notification.ServiceArea))
	if !ok {
		return object.NewError("Cannot find active handler for service " + msg.ServiceName() +
			" area " + strconv.Itoa(int(msg.ServiceArea())))
	}

	if msg != nil && msg.FailMessage() != "" {
		return h.Failed(pb, vnic, msg)
	}
	item, err := dcache.ItemOf(notification, this.resources)
	if err != nil {
		return object.NewError(err.Error())
	}
	npb := object.NewNotify(item)

	switch notification.Type {
	case types.NotificationType_Add:
		return h.Post(npb, vnic)
	case types.NotificationType_Replace:
		return h.Put(npb, vnic)
	case types.NotificationType_Sync:
		fallthrough
	case types.NotificationType_Update:
		return h.Patch(npb, vnic)
	case types.NotificationType_Delete:
		return h.Delete(npb, vnic)
	default:
		return object.NewError("invalid notification type, ignoring")
	}
}

func (this *ServiceManager) ServiceHandler(serviceName string, serviceArea byte) (ifs.IServiceHandler, bool) {
	return this.services.get(serviceName, serviceArea)
}

func (this *ServiceManager) RegisterDistributedCache(cache ifs.IDistributedCache) {
	key := cacheKey(cache.ServiceName(), cache.ServiceArea())
	this.distributedCaches.Put(key, cache)
}

func (this *ServiceManager) sendEndPoints(vnic ifs.IVNic) {
	webServices := this.services.webServices()
	for _, ws := range webServices {
		vnic.Resources().Logger().Info("Sent Webservice multicast for ", ws.ServiceName(), " area ", ws.ServiceArea())
		vnic.Multicast(ifs.WebService, 0, ifs.POST, ws.Serialize())
	}
}

func cacheKey(serviceName string, serviceArea byte) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}

func (this *ServiceManager) Services() *types.Services {
	return this.services.serviceList()
}
