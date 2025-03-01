package service_points

import (
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
	"google.golang.org/protobuf/proto"
)

func (this *ServicePointsImpl) performTransaction(msg *types.Message, vnic interfaces.IVirtualNetworkInterface) (proto.Message, error, bool) {
	hc := health.Health(vnic.Resources())
	leader := hc.Leader(msg.Type, msg.Vlan)
	uuid := vnic.Resources().Config().LocalUuid

	if leader != uuid && msg.Tr == nil {
		//This is a new transaction, forward to the leader
		r, err := vnic.Forward(msg, leader)
		resp, _ := r.(proto.Message)
		return resp, err, true
	} else if leader == uuid && msg.Tr == nil {
		msg.Tr = &types.Transaction{}
		msg.Tr.Id = interfaces.NewUuid()
		
		followers := hc.Uuids(msg.Topic, msg.Vlan)
		delete(followers, leader)
		for follower, _ := range followers {
			_, err := vnic.Forward(msg, follower)
			if err != nil {
				//@TODO - rollback
				return nil, err, true
			}
		}
	}
	return nil, nil, false
}
