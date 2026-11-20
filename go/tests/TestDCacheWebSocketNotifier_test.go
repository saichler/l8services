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

// DCache.SetWebSocketNotifier: a cache with no listener (l8inventory's case)
// forwards its targeted client notifications to the websocket service, and
// only for writes on a subscriber's displayed page
// (probler/plans/websocket-page-scoped-notifications.md Phase 2).

import (
	"fmt"
	"sync"
	"testing"

	"github.com/saichler/l8services/go/services/dcache"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/testtypes"
	"github.com/saichler/l8types/go/types/l8api"
	"github.com/saichler/l8types/go/types/l8notify"
)

// wsCaptureVnic records Multicast calls; every other IVNic method is unused
// here (the embedded nil interface would panic if one were called).
type wsCaptureVnic struct {
	ifs.IVNic
	mtx  sync.Mutex
	sent []*l8notify.L8NotificationSet
}

func (this *wsCaptureVnic) Multicast(serviceName string, serviceArea byte, action ifs.Action, data interface{}) error {
	if serviceName != "websock" || serviceArea != 0 {
		return fmt.Errorf("unexpected multicast to %s/%d", serviceName, serviceArea)
	}
	this.mtx.Lock()
	defer this.mtx.Unlock()
	this.sent = append(this.sent, data.(*l8notify.L8NotificationSet))
	return nil
}

func (this *wsCaptureVnic) take() []*l8notify.L8NotificationSet {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	s := this.sent
	this.sent = nil
	return s
}

func wsItem(i int) *testtypes.TestProto {
	return &testtypes.TestProto{MyString: fmt.Sprintf("ws-%05d", i), MyInt32: int32(i), MyFloat64: 1}
}

// registerPage registers aaaId's page [start, start+block) the way a live
// table's fetch does.
func registerPage(t *testing.T, cache ifs.IDistributedCache, aaaId string, start, block int) {
	t.Helper()
	elems, e := object.NewFromQuery(&l8api.L8Query{Text: "select * from TestProto", AaaId: aaaId,
		Register: l8api.L8Register_L8_REGISTER_REGISTER, RegisterId: "table"}, globals)
	if e != nil {
		t.Fatal(e)
	}
	q, e := elems.Query(globals)
	if e != nil {
		t.Fatal(e)
	}
	cache.Fetch(start, block, q)
}

func TestDCacheWebSocketNotifier(t *testing.T) {
	AddPrimaryKey(globals)
	cache := dcache.NewDistributedCache("WsNotifierTest", 0, &testtypes.TestProto{}, nil, nil, globals)
	for i := 0; i < 100; i++ {
		cache.Post(wsItem(i), true)
	}
	vnic := &wsCaptureVnic{}
	cache.(*dcache.DCache).SetWebSocketNotifier(vnic)
	registerPage(t, cache, "aaa-ws", 0, 15)

	// Off-page writes send nothing.
	off := wsItem(50)
	off.MyFloat64 = 2
	cache.Patch(off)
	cache.Delete(wsItem(60))
	if sent := vnic.take(); len(sent) != 0 {
		t.Fatalf("expected no websocket notifications for off-page writes, got %d", len(sent))
	}

	// An on-page patch sends one notification, targeted, with the record.
	on := wsItem(3)
	on.MyFloat64 = 2
	cache.Patch(on)
	sent := vnic.take()
	if len(sent) != 1 {
		t.Fatalf("expected 1 websocket notification for an on-page patch, got %d", len(sent))
	}
	if !sent[0].AaaIds["aaa-ws"] || len(sent[0].AaaIds) != 1 {
		t.Errorf("expected the notification to target only aaa-ws, got %v", sent[0].AaaIds)
	}
	if len(sent[0].NotificationList) == 0 || sent[0].NotificationList[0].NewValue == nil {
		t.Error("expected the notification to carry the record")
	}

	// On-page delete and replace (Post of an existing item) send one each.
	cache.Delete(wsItem(4))
	rep := wsItem(5)
	rep.MyFloat64 = 3
	cache.Post(rep)
	if sent := vnic.take(); len(sent) != 2 {
		t.Errorf("expected 2 websocket notifications for on-page delete+replace, got %d", len(sent))
	}

	// Replication (sourceNotification) writes never notify clients.
	rep.MyFloat64 = 4
	cache.Post(rep, true)
	if sent := vnic.take(); len(sent) != 0 {
		t.Errorf("expected no websocket notification for a replicated write, got %d", len(sent))
	}
}

// Without a websocket notifier and with no listener, nothing is sent -- the
// pre-existing behavior for a listener-less cache.
func TestDCacheNoWebSocketNotifier(t *testing.T) {
	AddPrimaryKey(globals)
	cache := dcache.NewDistributedCache("WsNotifierNone", 0, &testtypes.TestProto{}, nil, nil, globals)
	registerPage(t, cache, "aaa-ws", 0, 15)
	// Nothing to observe directly; this must simply not panic on a nil
	// listener and nil notifier.
	cache.Post(wsItem(1))
	cache.Patch(wsItem(1))
	cache.Delete(wsItem(1))
}
