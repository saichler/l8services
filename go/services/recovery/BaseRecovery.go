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

package recovery

import (
	"time"

	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8utils/go/utils/strings"
)

func RecoveryCheck(serviceName string, serviceArea byte, modelType string, nic ifs.IVNic) {
	time.Sleep(time.Second * 5)
	Sync(serviceName, serviceArea, modelType, nic)
	/*
		leader := nic.Resources().Services().GetLeader(serviceName, serviceArea)
		if nic.Resources().SysConfig().LocalUuid == leader {
			nic.Resources().Logger().Debug("Recover was called on leader, ignoring")
			return
		}
		gsql := "select * from " + modelType + " limit 1 page 0"
		resp := nic.Request(leader, serviceName, serviceArea, ifs.GET, gsql, 5)
		if resp == nil {
			return
		}
		if resp.Error() != nil {
			nic.Resources().Logger().Error("Recover: ", resp.Error().Error())
			return
		}

		list, _ := resp.AsList(nic.Resources().Registry())
		if list == nil {
			return
		}
		v := reflect.ValueOf(list)
		v = v.Elem()
		stats := v.FieldByName("Stats")
		if stats.IsValid() && stats.Kind() == reflect.Map {
			total := stats.MapIndex(reflect.ValueOf("Total"))
			if total.IsValid() {
				t := int(total.Int())
				if t != cache.Size() {
					nic.Resources().Logger().Error("Synching: ", serviceName, " area ", serviceArea,
						" local is ", cache.Size(), " total should be ", t)
					Sync(serviceName, serviceArea, cache, nic, t)
				}
			}
		}*/
}

func Sync(serviceName string, serviceArea byte, modelType string, nic ifs.IVNic) {
	leader := nic.Resources().Services().GetLeader(serviceName, serviceArea)
	handler, _ := nic.Resources().Services().ServiceHandler(serviceName, serviceArea)

	gsql := "select * from " + modelType + " limit 500 page "
	for i := 0; i <= 5000; i++ {
		qr := strings.New(gsql, i).String()
		resp := nic.Request(leader, serviceName, serviceArea, ifs.GET, qr, 15)
		if resp == nil {
			nic.Resources().Logger().Error("Sync: ", serviceName, " area ", serviceArea, " nil Response for page ", i)
			break
		}
		if resp.Error() != nil {
			nic.Resources().Logger().Error("Sync: ", serviceName, " area ", serviceArea,
				" error Response for page ", i, " ", resp.Error())
			break
		}
		if resp.Element() == nil {
			break
		}
		resp = object.NewNotify(resp.Elements())
		handler.Post(resp, nic)
	}
}
