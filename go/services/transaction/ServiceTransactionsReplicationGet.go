package transaction

import (
	"github.com/saichler/l8services/go/services/replication"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types"
	"github.com/saichler/layer8/go/overlay/health"
)

func replicationGet(elements ifs.IElements, servicePoints ifs.IServices, msg ifs.IMessage,
	vnic ifs.IVNic) ifs.IElements {
	index, _ := replication.ReplicationIndex(msg.ServiceName(), msg.ServiceArea(), vnic.Resources())
	if index != nil {
		servicePoint, _ := servicePoints.ServicePointHandler(msg.ServiceName(), msg.ServiceArea())
		// This is a replication service, we need to check if the key is not here
		key := servicePoint.TransactionMethod().KeyOf(elements, vnic.Resources())
		if key == "" {
			return getAll(elements, vnic, msg, index)
		}
		endpoints, ok := index.Keys[key]
		if !ok {
			return object.NewError("No Replica was found with key " + key)
		}
		_, here := endpoints.Location[vnic.Resources().SysConfig().LocalUuid]
		//The key is somewhere else, so forward the message there
		if !here {
			destination := ""
			for k, _ := range endpoints.Location {
				destination = k
				break
			}
			r := vnic.Forward(msg, destination)
			return r
		}
	}
	return nil
}

func getAll(elements ifs.IElements, vnic ifs.IVNic,
	msg ifs.IMessage, index *types.ReplicationIndex) ifs.IElements {
	myUuid := vnic.Resources().SysConfig().LocalUuid
	leader := health.Health(vnic.Resources()).Leader(msg.ServiceName(), msg.ServiceArea())
	isLeader := myUuid == leader
	if !isLeader && elements.ReplicasRequest() {
		return vnic.Resources().Services().TransactionHandle(elements, msg.Action(), vnic, msg)
	} else if !isLeader {
		return vnic.Forward(msg, leader)
	}

	request := object.NewReplicasRequest(elements)
	response := vnic.Resources().Services().TransactionHandle(elements, msg.Action(), vnic, msg)
	remotes := collectRemote(myUuid, index)
	//@TODO - Switch to parallel requests
	for dest, _ := range remotes {
		resp := vnic.Request(dest, msg.ServiceName(), msg.ServiceArea(), msg.Action(), request)
		response.Append(resp)
	}
	return response
}

func collectRemote(myUuid string, index *types.ReplicationIndex) map[string]bool {
	remote := make(map[string]bool)
	for _, v := range index.Keys {
		_, ok := v.Location[myUuid]
		if !ok {
			found := false
			for loc, _ := range v.Location {
				_, exist := v.Location[loc]
				if exist {
					found = true
					break
				}
			}
			if !found {
				for loc, _ := range v.Location {
					remote[loc] = true
					break
				}
			}
		}
	}
	return remote
}
