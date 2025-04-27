package service_points

import (
	"bytes"
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/servicepoints/go/points/dcache"
	"github.com/saichler/servicepoints/go/points/transaction"
	"github.com/saichler/shared/go/share/maps"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"strconv"
)

type ServicePointsImpl struct {
	services          *ServicesMap
	introspector      common.IIntrospector
	config            *types.SysConfig
	trManager         *transaction.TransactionManager
	distributedCaches *maps.SyncMap
}

func NewServicePoints(introspector common.IIntrospector, config *types.SysConfig) common.IServicePoints {
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

func (this *ServicePointsImpl) AddServicePointType(handler common.IServicePointHandler) {
	this.introspector.Registry().Register(handler)
}

func (this *ServicePointsImpl) Handle(pb common.IElements, action common.Action, vnic common.IVirtualNetworkInterface, msg common.IMessage) common.IElements {
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

	if msg.Action() == common.Sync {
		this.SyncDistributedCaches()
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

	if h.TransactionMethod() != nil && msg.Action() != common.GET {
		if common.IsNil(msg.Tr()) {
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

func (this *ServicePointsImpl) TransactionHandle(pb common.IElements, action common.Action, vnic common.IVirtualNetworkInterface, msg common.IMessage) common.IElements {
	h, _ := this.services.get(msg.ServiceName(), msg.ServiceArea())
	return this.handle(h, pb, action, vnic)
}

func (this *ServicePointsImpl) handle(h common.IServicePointHandler, pb common.IElements,
	action common.Action, vnic common.IVirtualNetworkInterface) common.IElements {

	if h == nil {
		return object.New(nil, pb)
	}

	var resourcs common.IResources

	if vnic != nil {
		resourcs = vnic.Resources()
	}

	switch action {
	case common.POST:
		return h.Post(pb, resourcs)
	case common.PUT:
		return h.Put(pb, resourcs)
	case common.PATCH:
		return h.Patch(pb, resourcs)
	case common.DELETE:
		return h.Delete(pb, resourcs)
	case common.GET:
		return h.Get(pb, resourcs)
	default:
		return object.NewError("invalid action, ignoring")
	}
}

func (this *ServicePointsImpl) Notify(pb common.IElements, vnic common.IVirtualNetworkInterface, msg common.IMessage, isTransaction bool) common.IElements {
	if vnic.Resources().SysConfig().LocalUuid == msg.Source() {
		return object.New(nil, nil)
	}
	notification := pb.Element().(*types.NotificationSet)
	h, ok := this.services.get(notification.ServiceName, uint16(notification.ServiceArea))
	if !ok {
		return object.NewError("Cannot find active handler for service " + msg.ServiceName() +
			" area " + strconv.Itoa(int(msg.ServiceArea())))
	}
	var resourcs common.IResources
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

func (this *ServicePointsImpl) ServicePointHandler(serviceName string, serviceArea uint16) (common.IServicePointHandler, bool) {
	return this.services.get(serviceName, serviceArea)
}

func (this *ServicePointsImpl) RegisterDistributedCache(cache common.IDistributedCache) {
	key := cacheKey(cache.ServiceName(), cache.ServiceArea())
	this.distributedCaches.Put(key, cache)
}

func (this *ServicePointsImpl) SyncDistributedCaches() {
	this.distributedCaches.Iterate(func(k, v interface{}) {
		v.(common.IDistributedCache).Sync()
	})
}

func cacheKey(serviceName string, serviceArea uint16) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}
