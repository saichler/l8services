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

	"github.com/saichler/l8reflect/go/reflect/introspecting"
	"github.com/saichler/l8reflect/go/reflect/updating"
	"github.com/saichler/l8srlz/go/serialize/object"
	. "github.com/saichler/l8test/go/infra/t_resources"
	"github.com/saichler/l8types/go/testtypes"
	"github.com/saichler/l8utils/go/utils/logger"
	"github.com/saichler/l8utils/go/utils/registry"
	"github.com/saichler/l8utils/go/utils/resources"
)

func TestSubStructProperty(t *testing.T) {
	res := resources.NewResources(logger.NewLoggerDirectImpl(&logger.FmtLogMethod{}))
	_introspect := introspecting.NewIntrospect(registry.NewRegistry())
	res.Set(_introspect)
	AddPrimaryKey(res)

	aside := &testtypes.TestProto{MyString: "Hello"}
	zside := &testtypes.TestProto{MyString: "Hello"}
	yside := &testtypes.TestProto{MyString: "Hello"}
	zside.MySingle = &testtypes.TestProtoSub{MyInt64: time.Now().Unix()}

	putUpdater := updating.NewUpdater(res, false, false)

	putUpdater.Update(aside, zside)

	changes := putUpdater.Changes()

	for _, change := range changes {
		obj := object.NewEncode()
		err := obj.Add(change.NewValue())
		if err != nil {
			Log.Fail(t, "failed with inspect: ", err.Error())
		}
		data := obj.Data()
		obj = object.NewDecode(data, 0, _introspect.Registry())
		newval, err := obj.Get()
		if err != nil {
			Log.Fail(t, "failed with inspect: ", err.Error())
		}
		v := newval.(*testtypes.TestProtoSub)
		if v.MyInt64 != zside.MySingle.MyInt64 {
			Log.Fail(t, "failed with inspect: myString != zside.MyString")
			return
		}
		if yside.MyString != zside.MyString {
			Log.Fail(t, "failed with inspect: myString != zside.MyString")
			return
		}
	}
}
