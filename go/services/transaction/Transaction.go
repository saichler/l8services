package transaction

import (
	"sort"
	"sync"
	"time"

	"github.com/saichler/l8services/go/services/replication"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8services"
	"github.com/saichler/layer8/go/overlay/health"
	"github.com/saichler/layer8/go/overlay/protocol"
)

type Transaction struct {
	msg          *ifs.Message
	vnic         ifs.IVNic
	preTrElement ifs.IElements
	cond         *sync.Cond
}

func NewTransaction(msg *ifs.Message, vnic ifs.IVNic) *Transaction {
	return &Transaction{msg: msg, vnic: vnic}
}

func (this *Transaction) SetVnic(vnic ifs.IVNic) {
	this.vnic = vnic
}

func (this *Transaction) Msg() *ifs.Message {
	return this.msg
}

func (this *Transaction) PreTrElement() ifs.IElements {
	return this.preTrElement
}

func (this *Transaction) SetPreTrElement(elements ifs.IElements) {
	this.preTrElement = elements
}

func (this *Transaction) Info(any ...interface{}) {
	this.vnic.Resources().Logger().Info(any...)
}

func (this *Transaction) Debug(any ...interface{}) {
	this.vnic.Resources().Logger().Debug(any...)
}

func (this *Transaction) Error(any ...interface{}) error {
	return this.vnic.Resources().Logger().Error(any...)
}

func (this *Transaction) Services() ifs.IServices {
	return this.vnic.Resources().Services()
}

func (this *Transaction) Security() ifs.ISecurityProvider {
	return this.vnic.Resources().Security()
}

func (this *Transaction) Resources() ifs.IResources {
	return this.vnic.Resources()
}

func (this *Transaction) Cond() *sync.Cond {
	if this.cond == nil {
		this.cond = sync.NewCond(&sync.Mutex{})
	}
	return this.cond
}

func (this *Transaction) VNic() ifs.IVNic {
	return this.vnic
}

func (this *Transaction) IsLeader() bool {
	healthCenter := health.Health(this.vnic.Resources())
	leader := healthCenter.LeaderFor(this.msg.ServiceName(), this.msg.ServiceArea())
	return leader == this.vnic.Resources().SysConfig().LocalUuid
}

func (this *Transaction) Leader() string {
	healthCenter := health.Health(this.vnic.Resources())
	return healthCenter.LeaderFor(this.msg.ServiceName(), this.msg.ServiceArea())
}

func (this *Transaction) Targets() map[string]bool {
	healthCenter := health.Health(this.vnic.Resources())
	targets := healthCenter.Uuids(this.msg.ServiceName(), this.msg.ServiceArea())
	delete(targets, this.vnic.Resources().SysConfig().LocalUuid)
	return targets
}

func (this *Transaction) TargetsWithReplication() (bool, bool, map[string]bool, map[string]bool) {
	healthCenter := health.Health(this.vnic.Resources())
	isLeader := healthCenter.LeaderFor(this.msg.ServiceName(), this.msg.ServiceArea()) == this.vnic.Resources().SysConfig().LocalUuid
	targets := healthCenter.Uuids(this.msg.ServiceName(), this.msg.ServiceArea())
	replicas := make(map[string]bool)
	for target, _ := range targets {
		replicas[target] = true
	}
	isLeaderATarget := true

	isReplication, leaderATarget, reps := this.replicationTargets()
	if isReplication {
		isLeaderATarget = leaderATarget
		replicas = reps
	}

	//Remove the leader from the targets & the replicas
	delete(targets, this.vnic.Resources().SysConfig().LocalUuid)
	delete(replicas, this.vnic.Resources().SysConfig().LocalUuid)

	return isLeader, isLeaderATarget, targets, replicas
}

func (this *Transaction) replicationTargets() (bool, bool, map[string]bool) {
	replicas := make(map[string]bool)
	isLeaderATarget := false
	service, _ := this.vnic.Resources().Services().ServiceHandler(this.msg.ServiceName(), this.msg.ServiceArea())
	isReplicationEnabled := service.TransactionConfig().Replication()
	replicationCount := service.TransactionConfig().ReplicationCount()
	if isReplicationEnabled && replicationCount > 0 {
		index, replicationService := replication.ReplicationIndex(this.msg.ServiceName(), this.msg.ServiceArea(), this.vnic.Resources())
		// if the replication count is larger than available replicas
		// warn and disable replication
		if len(index.EndPoints) < replicationCount {
			this.Error("Transaction.replicationTargets: Number of endpoint is smaller than replication count for service ",
				this.msg.ServiceArea(), " area ", this.msg.ServiceArea())
			return false, false, replicas
		}
		elems, err := protocol.ElementsOf(this.msg, this.vnic.Resources())
		if err != nil {
			panic(err)
		}
		key := service.TransactionConfig().KeyOf(elems, this.vnic.Resources())
		uuids, ok := index.Keys[key]
		if ok {
			for uuid, _ := range uuids.Location {
				replicas[uuid] = true
				index.Keys[key].Location[uuid] = time.Now().UnixMilli()
			}
		} else {
			endpoints := sortedEndpoints(index)
			replicationCounts := service.TransactionConfig().ReplicationCount()
			index.Keys[key] = &l8services.L8ReplicationKey{Location: make(map[string]int64)}
			for i := 0; i < replicationCounts; i++ {
				replicas[endpoints[i]] = true
				index.EndPoints[endpoints[i]].Score++
				index.Keys[key].Location[endpoints[i]] = time.Now().UnixMilli()
			}
			// Is the leader elected to be part of this commit
			_, isLeaderATarget = replicas[this.vnic.Resources().SysConfig().LocalUuid]
		}
		replication.UpdateIndex(replicationService, index)
		return true, isLeaderATarget, replicas
	}
	return false, false, replicas
}

func sortedEndpoints(index *l8services.L8ReplicationIndex) []string {
	endpoints := make([]string, len(index.EndPoints))
	i := 0
	for uuid, _ := range index.EndPoints {
		endpoints[i] = uuid
		i++
	}
	sort.Slice(endpoints, func(i, j int) bool {
		if index.EndPoints[endpoints[i]].Score <
			index.EndPoints[endpoints[j]].Score {
			return true
		}
		return false
	})
	return endpoints
}
