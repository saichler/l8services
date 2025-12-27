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
	"github.com/saichler/l8bus/go/overlay/protocol"
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_topology"
	. "github.com/saichler/l8types/go/ifs"
)

var topo *TestTopology
var globals, alias = CreateResources(50000, 1, Info_Level)

func init() {
	Log.SetLogLevel(Info_Level)
}

func setup() {
	setupTopology()
}

func tear() {
	shutdownTopology()
}

func reset(name string) {
	Log.Debug("*** ", name, " end ***")
	topo.ResetHandlers()
	Log.SetLogLevel(Info_Level)
}

func setupTopology() {
	protocol.MessageLog = true
	topo = NewTestTopology(4, []int{20000, 30000, 40000}, Info_Level)
}

func shutdownTopology() {
	topo.Shutdown()
}
