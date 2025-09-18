package states

import (
	"sync"

	"github.com/saichler/l8services/go/services/replication"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8services"
	"github.com/saichler/layer8/go/overlay/health"
)

func replicationGet(elements ifs.IElements, services ifs.IServices, msg *ifs.Message,
	vnic ifs.IVNic) ifs.IElements {
	index, _ := replication.ReplicationIndex(msg.ServiceName(), msg.ServiceArea(), vnic.Resources())
	if index != nil {
		service, _ := services.ServiceHandler(msg.ServiceName(), msg.ServiceArea())
		// This is a replication service, we need to check if the key is not here
		key := service.TransactionConfig().KeyOf(elements, vnic.Resources())
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
	msg *ifs.Message, index *l8services.L8ReplicationIndex) ifs.IElements {
	myUuid := vnic.Resources().SysConfig().LocalUuid
	leader := health.Health(vnic.Resources()).LeaderFor(msg.ServiceName(), msg.ServiceArea())
	isLeader := myUuid == leader
	if !isLeader && elements.ReplicasRequest() {
		return vnic.Resources().Services().TransactionHandle(elements, msg.Action(), vnic, msg)
	} else if !isLeader {
		return vnic.Forward(msg, leader)
	}

	request := object.NewReplicasRequest(elements)
	response := vnic.Resources().Services().TransactionHandle(elements, msg.Action(), vnic, msg)
	remotes := collectRemote(myUuid, index)

	wait := sync.WaitGroup{}
	mtx := sync.Mutex{}
	for dest, _ := range remotes {
		wait.Add(1)
		go func() {
			defer wait.Done()
			resp := vnic.Request(dest, msg.ServiceName(), msg.ServiceArea(), msg.Action(), request, int(msg.Tr_Timeout()))
			mtx.Lock()
			response.Append(resp)
			mtx.Unlock()
		}()
	}
	wait.Wait()
	return response
}

func collectRemote(myUuid string, index *l8services.L8ReplicationIndex) map[string]bool {
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
