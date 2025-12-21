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

	"github.com/saichler/l8reflect/go/tests/utils"
	"github.com/saichler/l8services/go/services/dcache"
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_service"
	"github.com/saichler/l8types/go/testtypes"
)

func TestCacheListener(t *testing.T) {
	item1 := utils.CreateTestModelInstance(1)
	AddPrimaryKey(globals)

	c := dcache.NewDistributedCache(ServiceName, 0, &testtypes.TestProto{}, nil, nil, globals)

	_, err := c.Put(item1)
	if err != nil {
		Log.Fail(t, err.Error())
		return
	}
	item2 := utils.CreateTestModelInstance(1)
	item2.MyEnum = testtypes.TestEnum_ValueTwo
	_, err = c.Patch(item2)
	if err != nil {
		Log.Fail(t, err.Error())
		return
	}
}
