package service_points

import (
	"errors"
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/servicepoints/go/points/cache"
	"github.com/saichler/servicepoints/go/points/transaction"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"strconv"
)

type ServicePointsImpl struct {
	services     *ServicesMap
	introspector common.IIntrospector
	config       *types.SysConfig
	trManager    *transaction.TransactionManager
}

func NewServicePoints(introspector common.IIntrospector, config *types.SysConfig) common.IServicePoints {
	sp := &ServicePointsImpl{}
	sp.services = NewServicesMap()
	sp.introspector = introspector
	sp.config = config
	sp.trManager = transaction.NewTransactionManager()
	_, err := introspector.Registry().Register(&types.NotificationSet{})
	if err != nil {
		panic(err)
	}
	return sp
}

func (this *ServicePointsImpl) AddServicePointType(handler common.IServicePointHandler) {
	this.introspector.Registry().Register(handler)
}

func (this *ServicePointsImpl) Activate(typeName string, serviceName string, serviceArea uint16,
	r common.IResources, l common.IServicePointCacheListener, args ...interface{}) (common.IServicePointHandler, error) {

	if typeName == "" {
		return nil, errors.New("typeName is empty")
	}

	if serviceName == "" {
		return nil, errors.New("Service name is empty")
	}

	info, err := this.introspector.Registry().Info(typeName)
	if err != nil {
		return nil, errors.New("Activate: " + err.Error())
	}
	h, err := info.NewInstance()
	if err != nil {
		return nil, errors.New("Activate: " + err.Error())
	}
	handler := h.(common.IServicePointHandler)
	err = handler.Activate(serviceName, serviceArea, r, l, args...)
	if err != nil {
		return nil, errors.New("Activate: " + err.Error())
	}
	this.services.put(serviceName, serviceArea, handler)
	common.AddService(this.config, serviceName, int32(serviceArea))
	vnic, ok := l.(common.IVirtualNetworkInterface)
	if ok {
		vnic.NotifyServiceAdded()
	}
	return handler, nil
}

func (this *ServicePointsImpl) DeActivate(serviceName string, serviceArea uint16, r common.IResources, l common.IServicePointCacheListener) error {

	if serviceName == "" {
		return errors.New("Service name is empty")
	}

	handler, ok := this.services.del(serviceName, serviceArea)
	if !ok {
		return errors.New("Can't find service " + serviceName)
	}

	defer handler.DeActivate()

	common.RemoveService(this.config.Services, serviceName, int32(serviceArea))
	vnic, ok := l.(common.IVirtualNetworkInterface)
	if ok {
		vnic.NotifyServiceRemoved(serviceName, serviceArea)
	}
	return nil
}

func (this *ServicePointsImpl) Handle(pb common.IElements, action common.Action, vnic common.IVirtualNetworkInterface, msg common.IMessage, insideTransaction bool) common.IElements {
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

	h, ok := this.services.get(msg.ServiceName(), msg.ServiceArea())
	if !ok {
		return object.NewError("Cannot find active handler for service " + msg.ServiceName() +
			" area " + strconv.Itoa(int(msg.ServiceArea())))
	}

	if msg.FailMessage() != "" {
		return h.Failed(pb, vnic.Resources(), msg)
	}

	if !insideTransaction {
		if h.Transactional() {
			if common.IsNil(msg.Tr()) {
				return this.trManager.Create(msg, vnic)
			} else {
				return this.trManager.Run(msg, vnic)
			}
		}
	}

	return this.doAction(h, action, msg.ServiceName(), msg.ServiceArea(), pb, vnic)

}

func (this *ServicePointsImpl) doAction(h common.IServicePointHandler, action common.Action,
	serviceName string, serviceArea uint16, pb common.IElements, vnic common.IVirtualNetworkInterface) common.IElements {

	if h == nil {
		return object.New(nil, pb)
	}

	var resourcs common.IResources

	if vnic != nil {
		resourcs = vnic.Resources()
	}

	switch action {
	case common.POST:
		if h.ReplicationCount() > 0 {
			healthCenter := health.Health(vnic.Resources())
			healthCenter.AddScore(vnic.Resources().SysConfig().LocalUuid, serviceName, serviceArea, vnic)
		}
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
	item, err := cache.ItemOf(notification, this.introspector)
	if err != nil {
		return object.NewError(err.Error())
	}
	npb := object.New(nil, item)

	switch notification.Type {
	case types.NotificationType_Add:
		return h.Post(npb, resourcs)
	case types.NotificationType_Replace:
		return h.Put(npb, resourcs)
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
