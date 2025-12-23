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

// Package base provides the foundational service implementation for the Layer 8 services framework.
// It implements core CRUD operations (Post, Put, Patch, Delete, Get) with integrated caching,
// transaction support, and notification handling.
package base

import (
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8web"
	"github.com/saichler/l8utils/go/utils/cache"
	"github.com/saichler/l8utils/go/utils/queues"
)

// BaseService provides the core service handler implementation that manages
// CRUD operations with caching, SLA enforcement, and notification support.
// It serves as the foundation for distributed services in the Layer 8 ecosystem.
type BaseService struct {
	cache   *cache.Cache
	vnic    ifs.IVNic
	sla     *ifs.ServiceLevelAgreement
	nQueue  *queues.Queue
	running bool
}

// Post creates new elements in the service cache. It delegates to the do method
// with the POST action, which handles callback invocations and notification generation.
func (this *BaseService) Post(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	return this.do(ifs.POST, pb, vnic)
}

// Put performs a full replacement update of elements in the service cache.
// It delegates to the do method with the PUT action.
func (this *BaseService) Put(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	return this.do(ifs.PUT, pb, vnic)
}

// Patch performs a partial update of elements in the service cache.
// Only the specified fields are modified while preserving other fields.
func (this *BaseService) Patch(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	return this.do(ifs.PATCH, pb, vnic)
}

// Delete removes elements from the service cache.
// It delegates to the do method with the DELETE action.
func (this *BaseService) Delete(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	return this.do(ifs.DELETE, pb, vnic)
}

// Get retrieves elements from the service cache. It supports two modes:
// - Filter mode: retrieves a single element matching the provided filter
// - Query mode: retrieves multiple elements with pagination support
// The SLA callback's Before hook is invoked prior to fetching data.
func (this *BaseService) Get(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	if this.sla.Callback() != nil {
		elem, cont, err := this.sla.Callback().Before(pb, ifs.GET, false, vnic)
		if err != nil {
			return object.NewError(err.Error())
		}
		if !cont {
			return object.New(nil, &l8web.L8Empty{})
		}
		if elem != nil {
			pb = elem.(ifs.IElements)
		}
	}
	if this.cache != nil {
		if pb.IsFilterMode() {
			e := this.validateElem(pb)
			if e != nil {
				return object.New(e, &l8web.L8Empty{})
			}
			resp, err := this.cache.Get(pb.Element())
			return object.New(err, resp)
		}
		q, e := pb.Query(this.vnic.Resources())
		if e != nil {
			return object.NewError(e.Error())
		}
		elems, counts := this.cache.Fetch(int(q.Page()*q.Limit()), int(q.Limit()), q)
		return object.NewQueryResult(elems, counts)
	}
	return pb
}

// Failed handles message delivery failures by logging an error.
// It is invoked when a message cannot be delivered to its intended recipient.
func (this *BaseService) Failed(pb ifs.IElements, vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	this.vnic.Resources().Logger().Error("Failed to deliver message")
	return nil
}

// TransactionConfig returns the transaction configuration for this service.
// Returns nil if the service is stateless or non-transactional; otherwise
// returns this service instance as the transaction configuration.
func (this *BaseService) TransactionConfig() ifs.ITransactionConfig {
	if !this.sla.Stateful() {
		return nil
	}
	if this.sla.Transactional() {
		return this
	}
	return nil
}

// WebService returns the web service interface from the SLA configuration,
// used for exposing the service via HTTP/REST endpoints.
func (this *BaseService) WebService() ifs.IWebService {
	return this.sla.WebService()
}

// Replication returns whether data replication is enabled for this service
// as specified in the SLA configuration.
func (this *BaseService) Replication() bool {
	return this.sla.Replication()
}

// ReplicationCount returns the number of replicas configured for this service
// as specified in the SLA configuration.
func (this *BaseService) ReplicationCount() int {
	return this.sla.ReplicationCount()
}

// KeyOf extracts and returns the primary key value from the given elements
// using the introspector's primary key decorator.
func (this *BaseService) KeyOf(elems ifs.IElements, r ifs.IResources) string {
	key, _, _ := r.Introspector().Decorators().PrimaryKeyDecoratorValue(elems.Element())
	return key
}

// Voter returns whether this service participates in leader election voting.
// BaseService always returns false; override in derived services for voting participation.
func (this *BaseService) Voter() bool {
	return false
}
