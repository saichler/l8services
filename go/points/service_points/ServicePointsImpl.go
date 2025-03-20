package service_points

import (
	"errors"
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/servicepoints/go/points/cache"
	"github.com/saichler/servicepoints/go/points/transaction"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"google.golang.org/protobuf/proto"
)

type ServicePointsImpl struct {
	multicast2ServicePoint *String2ServicePointMap
	introspector           common.IIntrospector
	config                 *types.VNicConfig
	trManager              *transaction.TransactionManager
}

func NewServicePoints(introspector common.IIntrospector, config *types.VNicConfig) common.IServicePoints {
	sp := &ServicePointsImpl{}
	sp.multicast2ServicePoint = NewString2ServicePointMap()
	sp.introspector = introspector
	sp.config = config
	sp.trManager = transaction.NewTransactionManager()
	_, err := introspector.Registry().Register(&types.NotificationSet{})
	if err != nil {
		panic(err)
	}
	return sp
}

func (this *ServicePointsImpl) RegisterServicePoint(multicast string, vlan int32, handler common.IServicePointHandler) error {
	if multicast == "" {
		return errors.New("cannot register handler with blank multicast group")
	}
	if handler == nil {
		return errors.New("cannot register nil handler for multicast group " + multicast)
	}
	_, err := this.introspector.Registry().Register(handler.SupportedProto())
	if err != nil {
		return err
	}
	this.multicast2ServicePoint.Put(multicast, handler)
	common.AddTopic(this.config, vlan, multicast)
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

	h, ok := this.multicast2ServicePoint.Get(msg.MulticastGroup)
	if !ok {
		return nil, errors.New("Cannot find handler for multicast group " + msg.MulticastGroup)
	}

	if msg.FailMsg != "" {
		return h.Failed(pb, vnic.Resources(), msg)
	}

	if !insideTransaction {
		if h.Transactional() {
			if msg.Tr == nil {
				return this.trManager.Start(msg, vnic)
			} else {
				return this.trManager.Run(msg, vnic)
			}
		}
	}

	resp, err := this.doAction(h, action, pb, vnic)

	return resp, err
}

func (this *ServicePointsImpl) doAction(h common.IServicePointHandler, action types.Action,
	pb proto.Message, vnic common.IVirtualNetworkInterface) (proto.Message, error) {

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
			healthCenter.AddScore(vnic.Resources().Config().LocalUuid, h.Multicast(), 0, vnic)
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
	h, ok := this.multicast2ServicePoint.Get(notification.MulticastGroup)
	if !ok {
		return nil, errors.New("Cannot find handler for multicast group " + notification.MulticastGroup)
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

func (this *ServicePointsImpl) ServicePointHandler(multicast string) (common.IServicePointHandler, bool) {
	return this.multicast2ServicePoint.Get(multicast)
}
