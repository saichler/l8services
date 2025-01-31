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
	structName2ServicePoint *String2ServicePointMap
	introspector            interfaces.IIntrospector
	config                  *types.VNicConfig
}

func NewServicePoints(introspector interfaces.IIntrospector, config *types.VNicConfig) interfaces.IServicePoints {
	sp := &ServicePointsImpl{}
	sp.structName2ServicePoint = NewString2ServicePointMap()
	sp.introspector = introspector
	sp.config = config
	return sp
}

func (servicePoints *ServicePointsImpl) RegisterServicePoint(pb proto.Message, handler interfaces.IServicePointHandler) error {
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
	servicePoints.structName2ServicePoint.Put(typ.Name(), handler)
	servicePoints.config.Topics[typ.Name()] = true
	return nil
}

func (servicePoints *ServicePointsImpl) Handle(pb proto.Message, action types.Action, vnic interfaces.IVirtualNetworkInterface, msg *types.Message) (proto.Message, error) {
	tName := reflect.ValueOf(pb).Elem().Type().Name()
	h, ok := servicePoints.structName2ServicePoint.Get(tName)
	if !ok {
		return nil, errors.New("Cannot find handler for type " + tName)
	}
	if msg != nil && msg.FailMsg != "" {
		return h.Failed(pb, vnic, msg)
	}
	switch action {
	case types.Action_POST:
		return h.Post(pb, vnic)
	case types.Action_PUT:
		return h.Put(pb, vnic)
	case types.Action_PATCH:
		return h.Patch(pb, vnic)
	case types.Action_DELETE:
		return h.Delete(pb, vnic)
	case types.Action_GET:
		return h.Get(pb, vnic)
	default:
		return nil, errors.New("invalid action, ignoring")
	}
}

func (servicePoints *ServicePointsImpl) Notify(pb proto.Message, action types.Action, vnic interfaces.IVirtualNetworkInterface, msg *types.Message) (proto.Message, error) {
	notification := pb.(*types.NotificationSet)
	h, ok := servicePoints.structName2ServicePoint.Get(notification.TypeName)
	if !ok {
		return nil, errors.New("Cannot find handler for type " + notification.TypeName)
	}
	if msg != nil && msg.FailMsg != "" {
		return h.Failed(pb, vnic, msg)
	}
	item, err := cache.ItemOf(notification, servicePoints.introspector)
	if err != nil {
		return nil, err
	}
	npb := item.(proto.Message)

	switch notification.Type {
	case types.NotificationType_Add:
		return h.Post(npb, vnic)
	case types.NotificationType_Replace:
		return h.Put(pb, vnic)
	case types.NotificationType_Update:
		return h.Patch(pb, vnic)
	case types.NotificationType_Delete:
		return h.Delete(pb, vnic)
	default:
		return nil, errors.New("invalid notification type, ignoring")
	}
}

func (servicePoints *ServicePointsImpl) ServicePointHandler(topic string) (interfaces.IServicePointHandler, bool) {
	return servicePoints.structName2ServicePoint.Get(topic)
}

func (servicePoints *ServicePointsImpl) Topics() map[string]bool {
	return servicePoints.structName2ServicePoint.Topics()
}
