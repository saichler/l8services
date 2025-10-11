package tests

import (
	"strconv"
	"testing"

	"github.com/saichler/l8services/go/services/replication"
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_service"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/testtypes"
)

func TestReplication(t *testing.T) {
	defer reset("TestReplication")
	if !doPostRound(1, 2, t) {
		return
	}
	if !doPostRound(1, 4, t) {
		return
	}
	if !doPostRound(2, 2, t) {
		return
	}
	if !doPostRound(2, 4, t) {
		return
	}

	nic := topo.VnicByVnetNum(1, 1)

	index := replication.ReplicationIndex("Tests", 2, nic.Resources())
	if len(index.Keys) != 2 {
		nic.Resources().Logger().Fail(t, "Replication Index should have 2 keys")
		return
	}

	pb := &testtypes.TestProto{MyString: "test" + strconv.Itoa(1)}
	resp := nic.Request("", ServiceName, 2, ifs.GET, pb, 5)
	if resp.Element().(*testtypes.TestProto).MyInt32 != 1 {
		nic.Resources().Logger().Fail(t, "Expected Attribute to be 1")
	}
}

func doPostRound(index, ecount int, t *testing.T) bool {
	pb := &testtypes.TestProto{MyString: "test" + strconv.Itoa(index), MyInt32: int32(index)}
	eg := topo.VnicByVnetNum(2, 1)
	resp := eg.ProximityRequest(ServiceName, 2, ifs.POST, pb, 5)

	if resp.Error() != nil {
		Log.Fail(t, resp.Error().Error())
		return false
	}

	count := 0
	handlers := topo.AllRepHandlers()
	for _, ts := range handlers {
		v, ok := ts.PostNReplica().Load(pb.MyString)
		if ok {
			count += v.(int)
		}
	}
	if count != ecount {
		Log.Fail(t, "Expected count 1 to be ", ecount, " got ", count)
		return false
	}
	return true
}
