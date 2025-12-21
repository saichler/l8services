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
	"testing"
	"time"

	"github.com/saichler/l8test/go/infra/t_service"
	"github.com/saichler/l8types/go/ifs"
)

func TestMapReduce(t *testing.T) {
	// Wait for all MapReduce participants to be ready
	if !waitForMapReduceParticipants(t) {
		return
	}

	nic := topo.VnicByVnetNum(1, 1)
	resp := nic.Request("", t_service.ServiceName, 0, ifs.MapR_GET, nil, 30)
	if resp.Error() != nil {
		nic.Resources().Logger().Fail(t, resp.Error().Error())
		return
	}
	if len(resp.Elements()) != 9 {
		nic.Resources().Logger().Fail(t, "Expected 9 elements, got", len(resp.Elements()))
		return
	}
}

func waitForMapReduceParticipants(t *testing.T) bool {
	// Wait for all 9 participants to be registered for MapReduce service area 0
	for i := 0; i < 10; i++ {
		allReady := true
		totalParticipants := 0

		// Check each vnic to see if it has the expected participants
		for vnet := 1; vnet <= 3; vnet++ {
			for vnic := 1; vnic <= 3; vnic++ {
				n := topo.VnicByVnetNum(vnet, vnic)
				if n == nil {
					allReady = false
					break
				}
				// Count participants for service area 0
				participants := n.Resources().Services().GetParticipants(t_service.ServiceName, 0)
				participantCount := len(participants)

				// For the first vnic, store the count
				if vnet == 1 && vnic == 1 {
					totalParticipants = participantCount
				}

				// All vnics should see the same number of participants
				if participantCount < 9 {
					allReady = false
					break
				}
			}
			if !allReady {
				break
			}
		}

		if allReady && totalParticipants >= 9 {
			// Give a bit more time for services to stabilize
			time.Sleep(200 * time.Millisecond)
			return true
		}

		time.Sleep(time.Second)
	}

	// Log current participant state for debugging
	n := topo.VnicByVnetNum(1, 1)
	if n != nil {
		participants := n.Resources().Services().GetParticipants(t_service.ServiceName, 0)
		n.Resources().Logger().Fail(t, "Timeout waiting for MapReduce participants. Current count:", len(participants))
	} else {
		t.Fatal("Could not get vnic for debugging")
	}
	return false
}
