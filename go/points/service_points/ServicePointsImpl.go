package service_points

import (
	"errors"
	"github.com/saichler/servicepoints/go/points/cache"
	"github.com/saichler/servicepoints/go/points/transaction"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"google.golang.org/protobuf/proto"
	"reflect"
)

type ServicePointsImpl struct {
	type2ServicePoint *String2ServicePointMap
	introspector      common.IIntrospector
	config            *types.VNicConfig
	trManager         *transaction.TransactionManager
}

func NewServicePoints(introspector common.IIntrospector, config *types.VNicConfig) common.IServicePoints {
	sp := &ServicePointsImpl{}
	sp.type2ServicePoint = NewString2ServicePointMap()
	sp.introspector = introspector
	sp.config = config
	sp.trManager = transaction.NewTransactionManager()
	introspector.Registry().Register(&types.NotificationSet{})
	return sp
}

func (this *ServicePointsImpl) RegisterServicePoint(vlan int32, pb proto.Message, handler common.IServicePointHandler) error {
	if pb == nil {
		return errors.New("cannot register handler with nil proto")
	}
	typ := reflect.ValueOf(pb).Elem().Type()
	if handler == nil {
		return errors.New("cannot register nil handler for type " + typ.Name())
	}
	_, err := this.introspector.Registry().RegisterType(typ)
	if err != nil {
		return err
	}
	this.type2ServicePoint.Put(typ.Name(), handler)
	common.AddTopic(this.config, vlan, typ.Name())
	return nil
}

func (this *ServicePointsImpl) Handle(pb proto.Message, action types.Action, vnic common.IVirtualNetworkInterface, msg *types.Message, insideTransaction bool) (proto.Message, error) {
	err := vnic.Resources().Security().CanDo()
	tName := reflect.ValueOf(pb).Elem().Type().Name()
	h, ok := this.type2ServicePoint.Get(tName)
	if !ok {
		return nil, errors.New("Cannot find handler for type " + tName)
	}
	var resourcs common.IResources
	if vnic != nil {
		resourcs = vnic.Resources()
	}

	if msg != nil && msg.FailMsg != "" {
		return h.Failed(pb, resourcs, msg)
	}

	if !insideTransaction {
		if h.Transactional() && resourcs != nil && msg != nil {
			if msg.Tr == nil {
				return this.trManager.Start(msg, vnic)
			} else {
				return this.trManager.Run(msg, vnic)
			}
		}
	}

	resp, err := this.doAction(h, action, pb, resourcs)

	return resp, err
}

func (this *ServicePointsImpl) doAction(h common.IServicePointHandler, action types.Action,
	pb proto.Message, resourcs common.IResources) (proto.Message, error) {
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

func (this *ServicePointsImpl) Notify(pb proto.Message, action types.Action, vnic common.IVirtualNetworkInterface, msg *types.Message, isTransaction bool) (proto.Message, error) {
	notification := pb.(*types.NotificationSet)
	h, ok := this.type2ServicePoint.Get(notification.TypeName)
	if !ok {
		return nil, errors.New("Cannot find handler for type " + notification.TypeName)
	}
	var resourcs common.IResources
	if vnic != nil {
		resourcs = vnic.Resources()
	}

	if msg != nil && msg.FailMsg != "" {
		return h.Failed(pb, resourcs, msg)
	}
	item, err := cache.ItemOf(notification, this.introspector)
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

func (this *ServicePointsImpl) ServicePointHandler(topic string) (common.IServicePointHandler, bool) {
	return this.type2ServicePoint.Get(topic)
}
