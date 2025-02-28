package service_points

import (
	"errors"
	"github.com/saichler/servicepoints/go/points/cache"
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
	"google.golang.org/protobuf/proto"
	"reflect"
)

type ServicePointsImpl struct {
	type2ServicePoint *String2ServicePointMap
	introspector      interfaces.IIntrospector
	config            *types.VNicConfig
}

func NewServicePoints(introspector interfaces.IIntrospector, config *types.VNicConfig) interfaces.IServicePoints {
	sp := &ServicePointsImpl{}
	sp.type2ServicePoint = NewString2ServicePointMap()
	sp.introspector = introspector
	sp.config = config
	introspector.Registry().Register(&types.NotificationSet{})
	return sp
}

func (servicePoints *ServicePointsImpl) RegisterServicePoint(area int32, pb proto.Message, handler interfaces.IServicePointHandler) error {
	if pb == nil {
		return errors.New("cannot register handler with nil proto")
	}
	typ := reflect.ValueOf(pb).Elem().Type()
	if handler == nil {
		return errors.New("cannot register nil handler for type " + typ.Name())
	}
	_, err := servicePoints.introspector.Registry().RegisterType(typ)
	if err != nil {
		return err
	}
	servicePoints.type2ServicePoint.Put(typ.Name(), handler)
	interfaces.AddTopic(servicePoints.config, area, typ.Name())
	return nil
}

func (servicePoints *ServicePointsImpl) Handle(pb proto.Message, action types.Action, vnic interfaces.IVirtualNetworkInterface, msg *types.Message) (proto.Message, error) {
	tName := reflect.ValueOf(pb).Elem().Type().Name()
	h, ok := servicePoints.type2ServicePoint.Get(tName)
	if !ok {
		return nil, errors.New("Cannot find handler for type " + tName)
	}
	var resourcs interfaces.IResources
	if vnic != nil {
		resourcs = vnic.Resources()
	}
	if msg != nil && msg.FailMsg != "" {
		return h.Failed(pb, resourcs, msg)
	}
	switch action {
	case types.Action_POST:
		return h.Post(pb, resourcs)
	case types.Action_PUT:
		return h.Put(pb, resourcs)
	case types.Action_PATCH:
		return h.Patch(pb, resourcs)
	case types.Action_DELETE:
		return h.Delete(pb, resourcs)
	case types.Action_GET:
		return h.Get(pb, resourcs)
	default:
		return nil, errors.New("invalid action, ignoring")
	}
}

func (servicePoints *ServicePointsImpl) Notify(pb proto.Message, action types.Action, vnic interfaces.IVirtualNetworkInterface, msg *types.Message) (proto.Message, error) {
	notification := pb.(*types.NotificationSet)
	h, ok := servicePoints.type2ServicePoint.Get(notification.TypeName)
	if !ok {
		return nil, errors.New("Cannot find handler for type " + notification.TypeName)
	}
	var resourcs interfaces.IResources
	if vnic != nil {
		resourcs = vnic.Resources()
	}
	
	if msg != nil && msg.FailMsg != "" {
		return h.Failed(pb, resourcs, msg)
	}
	item, err := cache.ItemOf(notification, servicePoints.introspector)
	if err != nil {
		return nil, err
	}
	npb := item.(proto.Message)

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
		return nil, errors.New("invalid notification type, ignoring")
	}
}

func (servicePoints *ServicePointsImpl) ServicePointHandler(topic string) (interfaces.IServicePointHandler, bool) {
	return servicePoints.type2ServicePoint.Get(topic)
}

func (servicePoints *ServicePointsImpl) ServiceAreas() *types.Areas {
	return servicePoints.config.ServiceAreas
}
