// © 2025 Sharon Aicler (saichler@gmail.com)
//
// Layer 8 Ecosystem is licensed under the Apache License, Version 2.0.
// You may obtain a copy of the License at:
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"strconv"
	"testing"
	"time"

	"github.com/saichler/l8services/go/services/replication"
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_service"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/testtypes"
)

func TestReplication(t *testing.T) {
	defer reset("TestReplication")

	// Wait for services to be ready for area 2
	if !waitForReplicationServices(t) {
		return
	}

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
	if resp.Error() != nil {
		nic.Resources().Logger().Fail(t, "GET request failed: ", resp.Error().Error())
		return
	}
	if resp.Element() == nil {
		nic.Resources().Logger().Fail(t, "GET request returned nil element")
		return
	}
	result, ok := resp.Element().(*testtypes.TestProto)
	if !ok {
		nic.Resources().Logger().Fail(t, "Element is not *testtypes.TestProto, got: ", resp.Element())
		return
	}
	if result.MyInt32 != 1 {
		nic.Resources().Logger().Fail(t, "Expected Attribute to be 1, got: ", result.MyInt32)
		return
	}
}

func waitForReplicationServices(t *testing.T) bool {
	// Wait for services to be ready on area 2
	for i := 0; i < 10; i++ {
		ok := true
		// Check if we have enough participants for area 2
		for vnet := 1; vnet <= 3; vnet++ {
			for vnic := 1; vnic <= 3; vnic++ {
				nic := topo.VnicByVnetNum(vnet, vnic)
				if nic == nil {
					continue
				}
				// Check if we have participants for Tests service area 2
				participants := nic.Resources().Services().GetParticipants(ServiceName, 2)
				if len(participants) < 2 {
					ok = false
					break
				}
			}
			if !ok {
				break
			}
		}
		if ok {
			// Give a bit more time for services to fully initialize
			time.Sleep(200 * time.Millisecond)
			return true
		}
		time.Sleep(time.Second)
	}
	Log.Fail(t, "Timeout waiting for replication services to be ready")
	return false
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
