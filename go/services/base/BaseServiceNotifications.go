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
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8notify"
	"github.com/saichler/l8types/go/types/l8web"
)

const (
	WsServiceName = "websock"
	WsServiceArea = byte(0)
)

// do executes a CRUD action (POST, PUT, PATCH, DELETE) on the provided elements.
// It invokes the SLA callback's Before and After hooks, performs the cache operation,
// and queues notifications for property changes when the service is stateful and voting.
func (this *BaseService) do(action ifs.Action, pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	createNotification := this.sla.Stateful() && this.sla.Voter() && !pb.Notification()
	if this.vnic != nil {
		vnic = this.vnic
	}

	elements := pb.Elements()
	// A query-mode DELETE (e.g. "delete from X where ...") carries no
	// discrete elements at all: pb.Elements() is always empty for it --
	// the query only ever populates pb's internal parsed-query field
	// (object.NewQuery/NewFromQuery), never a resolved element list -- so
	// without this, the loop below silently ran zero times and Delete
	// always reported success having deleted nothing. Resolve the query
	// into the real matching cache entries first, mirroring getQueried's
	// identical Fetch call for GET (same start/blockSize derivation from
	// the query's own Page/Limit), then let them flow through the exact
	// same per-element delete path below. Scoped to DELETE only -- POST
	// has no query-mode input to resolve (it only ever creates), and
	// query-mode PUT/PATCH (bulk update-by-query) is a separate, unasked
	// concern not addressed here.
	if action == ifs.DELETE && !pb.IsFilterMode() && this.cache != nil {
		q, e := pb.Query(vnic.Resources())
		if e != nil {
			return object.NewError(e.Error())
		}
		if q != nil {
			elements, _ = this.cache.Fetch(int(q.Page()*q.Limit()), int(q.Limit()), q)
		}
	}

	for _, elem := range elements {
		if elem == nil {
			continue
		}
		var n *l8notify.L8NotificationSet
		var cn *l8notify.L8NotificationSet
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
				n, cn, e = this.cache.Post(elem, createNotification)
			case ifs.PUT:
				n, cn, e = this.cache.Put(elem, createNotification)
			case ifs.PATCH:
				n, cn, e = this.cache.Patch(elem, createNotification)
			case ifs.DELETE:
				n, cn, e = this.cache.Delete(elem, createNotification)
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
		if this.nQueue != nil && createNotification && e == nil && n != nil {
			this.nQueue.Add(n)
		}
		if cn != nil && this.vnic != nil {
			this.vnic.Multicast(WsServiceName, WsServiceArea, ifs.Action(cn.Type), cn)
		}
	}
	return object.New(nil, &l8web.L8Empty{})
}

// processNotificationQueue runs as a background goroutine that continuously
// processes notification sets from the queue and broadcasts property change
// notifications via the virtual NIC. Stops when this.running becomes false.
func (this *BaseService) processNotificationQueue() {
	for this.running {
		set, ok := this.nQueue.Next().(*l8notify.L8NotificationSet)
		if ok {
			this.vnic.PropertyChangeNotification(set)
		}
	}
}

// Shutdown stops the service by setting running to false and adding a nil
// element to the notification queue to unblock the processNotificationQueue goroutine.
func (this *BaseService) Shutdown() {
	this.running = false
	if this.cache != nil && this.nQueue != nil {
		this.nQueue.Add(nil)
	}
}
