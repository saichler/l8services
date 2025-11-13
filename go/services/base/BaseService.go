package base

import (
	"reflect"

	"github.com/saichler/l8reflect/go/reflect/helping"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8web"
	"github.com/saichler/l8utils/go/utils/cache"
	"github.com/saichler/l8utils/go/utils/queues"
)

type BaseService struct {
	cache   *cache.Cache
	vnic    ifs.IVNic
	sla     *ifs.ServiceLevelAgreement
	nQueue  *queues.Queue
	running bool
}

func (this *BaseService) Post(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	return this.do(ifs.POST, pb, vnic)
}

func (this *BaseService) Put(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	return this.do(ifs.PUT, pb, vnic)
}

func (this *BaseService) Patch(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	return this.do(ifs.PATCH, pb, vnic)
}

func (this *BaseService) Delete(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
	return this.do(ifs.DELETE, pb, vnic)
}

func (this *BaseService) Get(pb ifs.IElements, vnic ifs.IVNic) ifs.IElements {
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
	elems := this.cache.Fetch(int(q.Page()*q.Limit()), int(q.Limit()), q)
	return object.NewQueryResult(elems, this.cache.Stats())
}

func (this *BaseService) Failed(pb ifs.IElements, vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	this.vnic.Resources().Logger().Error("Failed to deliver message")
	return nil
}

func (this *BaseService) TransactionConfig() ifs.ITransactionConfig {
	if !this.sla.Stateful() {
		return nil
	}
	if this.sla.Transactional() {
		return this
	}
	return nil
}

func (this *BaseService) WebService() ifs.IWebService {
	return this.sla.WebService()
}

func (this *BaseService) Replication() bool {
	return this.sla.Replication()
}

func (this *BaseService) ReplicationCount() int {
	return this.sla.ReplicationCount()
}

func (this *BaseService) KeyOf(elems ifs.IElements, r ifs.IResources) string {
	node, _ := r.Introspector().Node(this.cache.ModelType())
	key := helping.PrimaryDecorator(node, reflect.ValueOf(elems.Element()), this.vnic.Resources().Registry())
	return key.(string)
}

func (this *BaseService) Voter() bool {
	return false
}
