package service_points

import (
	"errors"
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/servicepoints/go/points/cache"
	"github.com/saichler/servicepoints/go/points/transaction"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"google.golang.org/protobuf/proto"
	"strconv"
)

type ServicePointsImpl struct {
	services     *ServicesMap
	introspector common.IIntrospector
	config       *types.VNicConfig
	trManager    *transaction.TransactionManager
}

func NewServicePoints(introspector common.IIntrospector, config *types.VNicConfig) common.IServicePoints {
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

func (this *ServicePointsImpl) RegisterServicePoint(handler common.IServicePointHandler, serviceArea int32) error {
	if handler == nil {
		return errors.New("cannot register a nil handler")
	}
	if handler.ServiceName() == "" {
		return errors.New("cannot register handler with blank Service Name")
	}

	if handler.ServiceModel() != nil {
		_, err := this.introspector.Registry().Register(handler.ServiceModel())
		if err != nil {
			return err
		}
	}
	this.services.Put(handler.ServiceName(), serviceArea, handler)
	common.AddService(this.config, handler.ServiceName(), serviceArea)
	return nil
}

func (this *ServicePointsImpl) Handle(pb proto.Message, action types.Action, vnic common.IVirtualNetworkInterface, msg *types.Message, insideTransaction bool) (proto.Message, error) {
	if vnic == nil {
		return nil, errors.New("Handle: vnic cannot be nil")
	}
	if msg == nil {
		return nil, errors.New("Handle: message cannot be nil")
	}
	err := vnic.Resources().Security().CanDoAction(action, pb, vnic.Resources().Config().LocalUuid, "")
	if err != nil {
		return nil, err
	}

	h, ok := this.services.Get(msg.ServiceName, msg.ServiceArea)
	if !ok {
		return nil, errors.New("Cannot find handler for service " + msg.ServiceName +
			" area " + strconv.Itoa(int(msg.ServiceArea)))
	}

	if msg.FailMsg != "" {
		return h.Failed(pb, vnic.Resources(), msg)
	}

	if !insideTransaction {
		if h.Transactional() {
			if msg.Tr == nil {
				return this.trManager.Create(msg, vnic)
			} else {
				return this.trManager.Run(msg, vnic)
			}
		}
	}

	resp, err := this.doAction(h, action, msg.ServiceArea, pb, vnic)

	return resp, err
}

func (this *ServicePointsImpl) doAction(h common.IServicePointHandler, action types.Action,
	serviceArea int32, pb proto.Message, vnic common.IVirtualNetworkInterface) (proto.Message, error) {

	if h == nil {
		return pb, nil
	}

	var resourcs common.IResources

	if vnic != nil {
		resourcs = vnic.Resources()
	}

	switch action {
	case types.Action_POST:
		if h.ReplicationCount() > 0 {
			healthCenter := health.Health(vnic.Resources())
			healthCenter.AddScore(vnic.Resources().Config().LocalUuid, h.ServiceName(), serviceArea, vnic)
		}
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

func (this *ServicePointsImpl) Notify(pb proto.Message, vnic common.IVirtualNetworkInterface, msg *types.Message, isTransaction bool) (proto.Message, error) {
	notification := pb.(*types.NotificationSet)
	h, ok := this.services.Get(notification.ServiceName, notification.ServiceArea)
	if !ok {
		return nil, errors.New("Cannot find handler for service " + msg.ServiceName +
			" area " + strconv.Itoa(int(msg.ServiceArea)))
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

func (this *ServicePointsImpl) ServicePointHandler(serviceName string, serviceArea int32) (common.IServicePointHandler, bool) {
	return this.services.Get(serviceName, serviceArea)
}
