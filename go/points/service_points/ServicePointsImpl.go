package service_points

import (
	"bytes"
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/servicepoints/go/points/dcache"
	"github.com/saichler/servicepoints/go/points/transaction"
	"github.com/saichler/l8utils/go/utils/maps"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types"
	"strconv"
)

type ServicePointsImpl struct {
	services          *ServicesMap
	introspector      ifs.IIntrospector
	config            *types.SysConfig
	trManager         *transaction.TransactionManager
	distributedCaches *maps.SyncMap
}

func NewServicePoints(introspector ifs.IIntrospector, config *types.SysConfig) ifs.IServices {
	sp := &ServicePointsImpl{}
	sp.services = NewServicesMap()
	sp.introspector = introspector
	sp.config = config
	sp.trManager = transaction.NewTransactionManager()
	sp.distributedCaches = maps.NewSyncMap()
	_, err := introspector.Registry().Register(&types.NotificationSet{})
	if err != nil {
		panic(err)
	}
	return sp
}

func (this *ServicePointsImpl) AddServicePointType(handler ifs.IServiceHandler) {
	this.introspector.Registry().Register(handler)
}

func (this *ServicePointsImpl) Handle(pb ifs.IElements, action ifs.Action, vnic ifs.IVirtualNetworkInterface, msg ifs.IMessage) ifs.IElements {
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

	h, ok := this.services.get(msg.ServiceName(), msg.ServiceArea())
	if !ok {
		return object.NewError("Cannot find active handler for service " + msg.ServiceName() +
			" area " + strconv.Itoa(int(msg.ServiceArea())))
	}

	if msg.FailMessage() != "" {
		return h.Failed(pb, vnic.Resources(), msg)
	}

	if h.TransactionMethod() != nil {
		if ifs.IsNil(msg.Tr()) {
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

func (this *ServicePointsImpl) TransactionHandle(pb ifs.IElements, action ifs.Action, vnic ifs.IVirtualNetworkInterface, msg ifs.IMessage) ifs.IElements {
	h, _ := this.services.get(msg.ServiceName(), msg.ServiceArea())
	return this.handle(h, pb, action, vnic)
}

func (this *ServicePointsImpl) handle(h ifs.IServiceHandler, pb ifs.IElements,
	action ifs.Action, vnic ifs.IVirtualNetworkInterface) ifs.IElements {

	if h == nil {
		return object.New(nil, pb)
	}

	var resourcs ifs.IResources

	if vnic != nil {
		resourcs = vnic.Resources()
	}

	switch action {
	case ifs.POST:
		return h.Post(pb, resourcs)
	case ifs.PUT:
		return h.Put(pb, resourcs)
	case ifs.PATCH:
		return h.Patch(pb, resourcs)
	case ifs.DELETE:
		return h.Delete(pb, resourcs)
	case ifs.GET:
		return h.Get(pb, resourcs)
	default:
		return object.NewError("invalid action, ignoring")
	}
}

func (this *ServicePointsImpl) Notify(pb ifs.IElements, vnic ifs.IVirtualNetworkInterface, msg ifs.IMessage, isTransaction bool) ifs.IElements {
	if vnic.Resources().SysConfig().LocalUuid == msg.Source() {
		return object.New(nil, nil)
	}
	notification := pb.Element().(*types.NotificationSet)
	h, ok := this.services.get(notification.ServiceName, uint16(notification.ServiceArea))
	if !ok {
		return object.NewError("Cannot find active handler for service " + msg.ServiceName() +
			" area " + strconv.Itoa(int(msg.ServiceArea())))
	}
	var resourcs ifs.IResources
	if vnic != nil {
		resourcs = vnic.Resources()
	}

	if msg != nil && msg.FailMessage() != "" {
		return h.Failed(pb, resourcs, msg)
	}
	item, err := dcache.ItemOf(notification, this.introspector)
	if err != nil {
		return object.NewError(err.Error())
	}
	npb := object.NewNotify(item)

	switch notification.Type {
	case types.NotificationType_Add:
		return h.Post(npb, resourcs)
	case types.NotificationType_Replace:
		return h.Put(npb, resourcs)
	case types.NotificationType_Sync:
		fallthrough
	case types.NotificationType_Update:
		return h.Patch(npb, resourcs)
	case types.NotificationType_Delete:
		return h.Delete(npb, resourcs)
	default:
		return object.NewError("invalid notification type, ignoring")
	}
}

func (this *ServicePointsImpl) ServicePointHandler(serviceName string, serviceArea uint16) (ifs.IServiceHandler, bool) {
	return this.services.get(serviceName, serviceArea)
}

func (this *ServicePointsImpl) RegisterDistributedCache(cache ifs.IDistributedCache) {
	key := cacheKey(cache.ServiceName(), cache.ServiceArea())
	this.distributedCaches.Put(key, cache)
}

func cacheKey(serviceName string, serviceArea uint16) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}
