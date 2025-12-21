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

package base

import (
	"fmt"

	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8notify"
	"github.com/saichler/l8types/go/types/l8web"
)

func (this *BaseService) do(action ifs.Action, pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	createNotification := this.sla.Stateful() && this.sla.Voter() && !pb.Notification()
	if this.vnic != nil {
		vnic = this.vnic
	}
	for _, elem := range pb.Elements() {
		if elem == nil {
			continue
		}
		var n *l8notify.L8NotificationSet
		var e error
		if this.sla.Callback() != nil {
			beforElem, cont, err := this.sla.Callback().Before(elem, action, pb.Notification(), vnic)
			if err != nil {
				return object.NewError(err.Error())
			}
			if !cont {
				return object.New(nil, &l8web.L8Empty{})
			}
			if beforElem != nil {
				elem = beforElem
			}
		}
		if this.cache != nil {
			switch action {
			case ifs.POST:
				n, e = this.cache.Post(elem, createNotification)
			case ifs.PUT:
				n, e = this.cache.Put(elem, createNotification)
			case ifs.PATCH:
				n, e = this.cache.Patch(elem, createNotification)
			case ifs.DELETE:
				n, e = this.cache.Delete(elem, createNotification)
			}
		}
		if this.sla.Callback() != nil {
			if action == ifs.PATCH && this.cache != nil {
				elem, _ = this.cache.Get(elem)
			}
			afterElem, cont, err := this.sla.Callback().After(elem, action, pb.Notification(), vnic)
			if err != nil {
				return object.NewError(err.Error())
			}
			if !cont {
				return object.New(nil, &l8web.L8Empty{})
			}
			if afterElem != nil {
				elem = afterElem
			}
		}
		if e != nil {
			fmt.Println("Error in notification: ", e.Error())
		}
		if createNotification && e == nil && n != nil {
			this.nQueue.Add(n)
		}
	}
	return object.New(nil, &l8web.L8Empty{})
}

func (this *BaseService) processNotificationQueue() {
	for this.running {
		set, ok := this.nQueue.Next().(*l8notify.L8NotificationSet)
		if ok {
			this.vnic.PropertyChangeNotification(set)
		}
	}
}

func (this *BaseService) Shutdown() {
	this.running = false
	if this.cache != nil {
		this.nQueue.Add(nil)
	}
}
