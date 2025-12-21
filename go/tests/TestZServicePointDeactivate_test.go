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

	"github.com/saichler/l8bus/go/overlay/health"
	. "github.com/saichler/l8test/go/infra/t_resources"
	"github.com/saichler/l8test/go/infra/t_service"
)

// Until i find the deactivate bug, run this test at the end
func testZServiceDeactivate(t *testing.T) {
	nic := topo.VnicByVnetNum(2, 2)
	nic2 := topo.VnicByVnetNum(1, 3)
	WaitForCondition(func() bool {
		hp := health.HealthOf(nic.Resources().SysConfig().LocalUuid, nic.Resources())
		for k, _ := range hp.Services.ServiceToAreas {
			if k == "Tests" {
				return true
			}
		}
		return false
	}, 5, t, "Service  was not found")

	nic.Resources().Services().DeActivate(t_service.ServiceName, 0, nic.Resources(), nic)
	defer func() {
		topo.ReActivateTestService(nic)
	}()

	WaitForCondition(func() bool {
		hp := health.HealthOf(nic.Resources().SysConfig().LocalUuid, nic2.Resources())
		for k, _ := range hp.Services.ServiceToAreas {
			if k == "Tests" {
				return false
			}
		}
		return true
	}, 5, t, "Service  exist")

}
