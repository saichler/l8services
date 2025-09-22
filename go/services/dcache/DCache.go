package dcache

import (
	"errors"
	"reflect"
	"sync"

	"github.com/saichler/l8reflect/go/reflect/cloning"
	"github.com/saichler/l8reflect/go/reflect/introspecting"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8utils/go/utils/strings"
)

type DCache struct {
	cache         *localCache
	mtx           *sync.RWMutex
	cond          *sync.Cond
	listener      ifs.IServiceCacheListener
	cloner        *cloning.Cloner
	resources     ifs.IResources
	source        string
	serviceName   string
	serviceArea   byte
	modelType     string
	sequence      uint32
	store         ifs.IStorage
	keyFieldNames []string
}

func NewDistributedCache(serviceName string, serviceArea byte, sample interface{}, initElements []interface{},
	listener ifs.IServiceCacheListener, resources ifs.IResources) ifs.IDistributedCache {
	return NewDistributedCacheWithStorage(serviceName, serviceArea, sample, initElements, listener, resources, nil, false)
}

func NewDistributedCacheNoSync(serviceName string, serviceArea byte, sample interface{}, initElements []interface{},
	listener ifs.IServiceCacheListener, resources ifs.IResources) ifs.IDistributedCache {
	return NewDistributedCacheWithStorage(serviceName, serviceArea, sample, initElements, listener, resources, nil, true)
}

func NewDistributedCacheWithStorage(serviceName string, serviceArea byte, sample interface{}, initElements []interface{},
	listener ifs.IServiceCacheListener, resources ifs.IResources, store ifs.IStorage, noSync bool) ifs.IDistributedCache {
	this := &DCache{}
	this.cache = newLocalCache()
	this.mtx = &sync.RWMutex{}
	this.cond = sync.NewCond(this.mtx)
	this.listener = listener
	this.cloner = cloning.NewCloner()
	this.resources = resources
	this.source = resources.SysConfig().LocalUuid
	this.serviceName = serviceName
	this.serviceArea = serviceArea
	this.store = store
	_, err := this.PrimaryKeyFor(sample)
	if err != nil {
		panic(err)
	}

	if this.store != nil {
		items := this.store.Collect(all)
		for k, v := range items {
			this.cache.put(k, v)
		}
	} else if initElements != nil {
		resources.Logger().Info("Distributed cache - Adding initialized elements")
		for _, item := range initElements {
			k, er := this.PrimaryKeyFor(item)
			if er != nil {
				resources.Logger().Info(er.Error())
				continue
			}
			this.cache.put(k, item)
		}
	}

	if listener != nil && !noSync {
		resources.Services().RegisterDistributedCache(this)
	}
	return this
}

func (this *DCache) ServiceName() string {
	return this.serviceName
}

func (this *DCache) ServiceArea() byte {
	return this.serviceArea
}

func (this *DCache) cacheEnabled() bool {
	if this.store == nil {
		return true
	}
	return this.store.CacheEnabled()
}

func (this *DCache) Size() int {
	this.mtx.RLock()
	defer this.mtx.RUnlock()
	return this.cache.size()
}

func (this *DCache) typeFor(any interface{}) (string, error) {
	if this.modelType != "" {
		return this.modelType, nil
	}

	if any == nil {
		return "", errors.New("Cannot get type for nil interface")
	}

	v := reflect.ValueOf(any)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	this.modelType = v.Type().Name()

	return this.modelType, nil
}

func (this *DCache) PrimaryKeyFor(any interface{}) (string, error) {
	if any == nil {
		return "", errors.New("Cannot get key for nil interface")
	}

	v := reflect.ValueOf(any)
	if v.Kind() != reflect.Ptr {
		return "", errors.New("Cannot get key for non-pointer interface")
	}
	v = v.Elem()

	if this.keyFieldNames == nil {
		typ, err := this.typeFor(any)
		if err != nil {
			return "", err
		}
		node, ok := this.resources.Introspector().Node(typ)
		if !ok {
			return "", errors.New("Could not find an interospector node for type " + typ)
		}
		pk := introspecting.PrimaryKeyDecorator(node)
		if pk == nil {
			return "", errors.New("No primary key decorator is defined for type " + typ)
		}
		this.keyFieldNames = pk.([]string)
	}

	if len(this.keyFieldNames) == 0 {
		return "", errors.New("Lost of keys is empty for type " + this.modelType)
	} else if len(this.keyFieldNames) == 1 {
		return strings.New(v.FieldByName(this.keyFieldNames[0]).Interface()).String(), nil
	} else if len(this.keyFieldNames) == 2 {
		return strings.New(v.FieldByName(this.keyFieldNames[0]).Interface(),
			v.FieldByName(this.keyFieldNames[1]).Interface()).String(), nil
	} else if len(this.keyFieldNames) == 3 {
		return strings.New(v.FieldByName(this.keyFieldNames[0]).Interface(),
			v.FieldByName(this.keyFieldNames[1]).Interface()).String(), nil
	}
	result := strings.New()
	for i := 0; i < len(this.keyFieldNames); i++ {
		result.Add(result.StringOf(v.FieldByName(this.keyFieldNames[0]).Interface()))
	}
	return result.String(), nil
}
