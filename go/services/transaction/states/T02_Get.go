package states

import (
	"github.com/saichler/l8bus/go/overlay/protocol"
	"github.com/saichler/l8services/go/services/replication"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
)

func (this *ServiceTransactions) internalGet(msg *ifs.Message) ifs.IElements {
	pb, err := protocol.ElementsOf(msg, this.nic.Resources())
	if err != nil {
		msg.SetTr_State(ifs.Failed)
		msg.SetTr_ErrMsg("T04_Commit.commitInternal: Protocol Error: " + msg.Tr_Id() + " " + err.Error())
		this.nic.Resources().Logger().Debug(msg.Tr_Id() + " " + err.Error())
		return L8TransactionFor(msg)
	}

	service, _ := this.nic.Resources().Services().ServiceHandler(msg.ServiceName(), msg.ServiceArea())
	if service.TransactionConfig().Replication() {
		return this.replicationGet(pb, msg, service)
	}

	return this.nic.Resources().Services().TransactionHandle(pb, msg.Action(), msg, this.nic)
}

func (this *ServiceTransactions) replicationGet(pb ifs.IElements, msg *ifs.Message, service ifs.IServiceHandler) ifs.IElements {

	//This is the node that has the requested replica
	if msg.Tr_IsReplica() {
		pb = object.NewReplicaRequest(pb, msg.Tr_Replica())
		return this.nic.Resources().Services().TransactionHandle(pb, msg.Action(), msg, this.nic)
	}

	//if this a filter mode
	if pb.IsFilterMode() {
		return this.replicationGetFilter(pb, msg, service)
	}

	//@TODO replication get query

	return this.nic.Resources().Services().TransactionHandle(pb, msg.Action(), msg, this.nic)
}

func (this ServiceTransactions) replicationGetFilter(pb ifs.IElements, msg *ifs.Message, service ifs.IServiceHandler) ifs.IElements {
	index := replication.ReplicationIndex(msg.ServiceName(), msg.ServiceArea(), this.nic.Resources())

	//index is healthy, we can use replica 0
	if index.Extracted == nil || len(index.Extracted) == 0 {
		key := service.TransactionConfig().KeyOf(pb, this.nic.Resources())
		replicas, ok := index.Keys[key]
		if ok {
			if replicas.Replica0 == "" {
				for uuid, replica := range replicas.Location {
					if replica == 0 {
						replicas.Replica0 = uuid
						break
					}
				}
			}
			msg.SetTr_IsReplica(true)
			return this.nic.Forward(msg, replicas.Replica0)
		}
		return object.NewError("Replica for key " + key + " Not found")
	}

	return this.nic.Resources().Services().TransactionHandle(pb, msg.Action(), msg, this.nic)
}
