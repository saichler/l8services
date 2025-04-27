package transaction

import (
	"github.com/saichler/layer8/go/overlay/protocol"
	"github.com/saichler/servicepoints/go/points/replication"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
	"sort"
	"time"
)

func replicationTargets(vnic common.IVirtualNetworkInterface, msg common.IMessage) (bool, bool, map[string]bool) {
	replicas := make(map[string]bool)
	isLeaderATarget := false
	servicePoint, _ := vnic.Resources().ServicePoints().ServicePointHandler(msg.ServiceName(), msg.ServiceArea())
	isReplicationEnabled := servicePoint.TransactionMethod().Replication()
	replicationCount := servicePoint.TransactionMethod().ReplicationCount()
	if isReplicationEnabled && replicationCount > 0 {
		index, replicationServicePoint := replication.ReplicationIndex(msg.ServiceName(), msg.ServiceArea(), vnic.Resources())
		// if the replication count is larger than available replicas
		// warn and disable replication
		if len(index.EndPoints) < replicationCount {
			vnic.Resources().Logger().Warning("Number of endpoint is smaller than replication count for service ",
				msg.ServiceArea(), " area ", msg.ServiceArea())
			return false, false, replicas
		}
		elems, err := protocol.ElementsOf(msg, vnic.Resources())
		if err != nil {
			panic(err)
		}
		key := servicePoint.TransactionMethod().KeyOf(elems)
		uuids, ok := index.Keys[key]
		if ok {
			for uuid, _ := range uuids.Location {
				replicas[uuid] = true
				index.Keys[key].Location[uuid] = time.Now().UnixMilli()
			}
		} else {
			endpoints := sortedEndpoints(index)
			replicationCount := servicePoint.TransactionMethod().ReplicationCount()
			index.Keys[key] = &types.ReplicationKey{Location: make(map[string]int64)}
			for i := 0; i < replicationCount; i++ {
				replicas[endpoints[i]] = true
				index.EndPoints[endpoints[i]].Score++
				index.Keys[key].Location[endpoints[i]] = time.Now().UnixMilli()
			}
			// Is the leader elected to be part of this commit
			_, isLeaderATarget = replicas[vnic.Resources().SysConfig().LocalUuid]
		}
		replication.UpdateIndex(replicationServicePoint, index)
		return true, isLeaderATarget, replicas
	}
	return false, false, replicas
}

func sortedEndpoints(index *types.ReplicationIndex) []string {
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
