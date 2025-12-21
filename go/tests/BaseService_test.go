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
	"fmt"
	"testing"
	"time"

	"github.com/saichler/l8reflect/go/tests/utils"
	"github.com/saichler/l8services/go/services/base"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/testtypes"
)

func TestBaseService(t *testing.T) {
	sc := ifs.NewServiceLevelAgreement(&base.BaseService{}, "base", 0, true, nil)
	sc.SetServiceItem(&testtypes.TestProto{})
	sc.SetServiceItemList(&testtypes.TestProtoList{})
	sc.SetPrimaryKeys("MyString")
	sc.SetVoter(true)

	for vnet := 1; vnet <= 3; vnet++ {
		for vnic := 1; vnic <= 3; vnic++ {
			nic := topo.VnicByVnetNum(vnet, vnic)
			base.Activate(sc, nic)
		}
	}

	time.Sleep(time.Second)
	nic := topo.VnicByVnetNum(1, 1)
	for i := 0; i < 10; i++ {
		elem := utils.CreateTestModelInstance(i)
		h, _ := nic.Resources().Services().ServiceHandler(sc.ServiceName(), 0)
		h.Post(object.New(nil, elem), nic)
	}

	time.Sleep(time.Second)

	for vnet := 1; vnet <= 3; vnet++ {
		for vnic := 1; vnic <= 3; vnic++ {
			nic = topo.VnicByVnetNum(vnet, vnic)
			h, _ := nic.Resources().Services().ServiceHandler(sc.ServiceName(), 0)
			hb := h.(*base.BaseService)
			if hb.Size() != 10 {
				fmt.Println(nic.Resources().SysConfig().LocalAlias, " does not have 10 items ", hb.Size())
				t.Fail()
				return
			}
		}
	}

	time.Sleep(time.Second * 10)

	topo.RenewVnic(2, 3)

	vnic := topo.VnicByVnetNum(2, 3)
	base.Activate(sc, vnic)

	time.Sleep(time.Second * 10)

	nic = topo.VnicByVnetNum(2, 3)
	h, _ := nic.Resources().Services().ServiceHandler(sc.ServiceName(), 0)
	hb := h.(*base.BaseService)
	if hb.Size() != 10 {
		fmt.Println("recover ", nic.Resources().SysConfig().LocalAlias, " does not have 10 items ", hb.Size())
		t.Fail()
		return
	}
}
