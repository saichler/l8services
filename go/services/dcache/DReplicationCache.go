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

package dcache

import (
	"sync"

	"github.com/saichler/l8types/go/ifs"
)

type ReplicationCache struct {
	cache    map[int]ifs.IDistributedCache
	mtx      *sync.Mutex
	resource ifs.IResources
	store    ifs.IStorage
}

func NewReplicationCache(r ifs.IResources, store ifs.IStorage) ifs.IReplicationCache {
	c := &ReplicationCache{}
	c.cache = make(map[int]ifs.IDistributedCache)
	c.mtx = &sync.Mutex{}
	c.resource = r
	c.store = store
	return c
}

func (this *ReplicationCache) getCache(elem interface{}, rep int) ifs.IDistributedCache {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	repCache, ok := this.cache[rep]
	if !ok {
		repCache = NewDistributedCacheWithStorage("", 0, elem,
			nil, nil, this.resource, this.store)
		this.cache[rep] = repCache
	}
	return repCache
}

func (this *ReplicationCache) Post(elem interface{}, rep int) error {
	repCache := this.getCache(elem, rep)
	_, err := repCache.Post(elem, true)
	return err
}

func (this *ReplicationCache) Put(elem interface{}, rep int) error {
	repCache := this.getCache(elem, rep)
	_, err := repCache.Put(elem, true)
	return err
}

func (this *ReplicationCache) Patch(elem interface{}, rep int) error {
	repCache := this.getCache(elem, rep)
	_, err := repCache.Patch(elem, true)
	return err
}

func (this *ReplicationCache) Delete(elem interface{}, rep int) error {
	repCache := this.getCache(elem, rep)
	_, err := repCache.Delete(elem, true)
	return err
}

func (this *ReplicationCache) Get(elem interface{}, rep int) (interface{}, error) {
	repCache := this.getCache(elem, rep)
	return repCache.Get(elem)
}
