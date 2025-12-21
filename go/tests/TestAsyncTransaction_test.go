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

	"github.com/saichler/l8types/go/ifs"
)

func TestAsyncTransaction(t *testing.T) {
	topo.SetLogLevel(ifs.Debug_Level)
	defer reset("TestTransaction")

	eg2_2 := topo.VnicByVnetNum(2, 2)
	eg1_1 := topo.VnicByVnetNum(1, 1)

	if !doAsyncTransaction(ifs.POST, eg2_2, 1, t, true) {
		return
	}

	if !doAsyncTransaction(ifs.POST, eg2_2, 2, t, true) {
		return
	}

	if !doAsyncTransaction(ifs.POST, eg1_1, 3, t, true) {
		return
	}

}
