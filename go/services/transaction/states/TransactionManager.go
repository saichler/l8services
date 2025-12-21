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

package states

import (
	"sync"

	"github.com/saichler/l8types/go/ifs"
)

type TransactionManager struct {
	serviceTransactions map[string]*ServiceTransactions
	services            ifs.IServices
	mtx                 *sync.Mutex
}

func NewTransactionManager(services ifs.IServices) *TransactionManager {
	tm := &TransactionManager{}
	tm.mtx = &sync.Mutex{}
	tm.services = services
	tm.serviceTransactions = make(map[string]*ServiceTransactions)
	return tm
}

func (this *TransactionManager) transactionsOf(msg *ifs.Message, nic ifs.IVNic) *ServiceTransactions {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	serviceKey := ServiceKey(msg.ServiceName(), msg.ServiceArea())
	st, ok := this.serviceTransactions[serviceKey]
	if !ok {
		this.serviceTransactions[serviceKey] = newServiceTransactions(nic)
		st = this.serviceTransactions[serviceKey]
	}
	return st
}

func (this *TransactionManager) Run(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	switch msg.Tr_State() {
	case ifs.Created:
		return this.created(msg, vnic)
	case ifs.Running:
		return this.commit(msg, vnic)
	case ifs.Rollback:
		return this.rollback(msg, vnic)
	case ifs.Cleanup:
		return this.cleanup(msg, vnic)
	default:
		panic("Unexpected transaction state " + msg.Tr_State().String() + ":" + msg.Tr_ErrMsg())
	}
}

// First we insert the transaction to the Queue and mark it as queued
func (this *TransactionManager) created(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	st := this.transactionsOf(msg, vnic)
	return st.queueTransaction(msg, vnic)
}

func (this *TransactionManager) commit(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	st := this.transactionsOf(msg, vnic)
	return st.commitInternal(msg)
}

func (this *TransactionManager) rollback(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	st := this.transactionsOf(msg, vnic)
	return st.rollbackInternal(msg)
}

func (this *TransactionManager) cleanup(msg *ifs.Message, vnic ifs.IVNic) ifs.IElements {
	st := this.transactionsOf(msg, vnic)
	return st.cleanupInternal(msg)
}
