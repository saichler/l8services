package base

import (
	"errors"
	"reflect"

	"github.com/saichler/l8reflect/go/reflect/introspecting"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8api"
	"github.com/saichler/l8types/go/types/l8web"
	"github.com/saichler/l8utils/go/utils/cache"
	"github.com/saichler/l8utils/go/utils/queues"
)

func Activate(sla *ifs.ServiceLevelAgreement, vnic ifs.IVNic) error {
	vnic.Resources().Registry().Register(&BaseService{})
	vnic.Resources().Registry().Register(sla.ServiceItemList())
	vnic.Resources().Registry().Register(&l8web.L8Empty{})
	vnic.Resources().Registry().Register(&l8api.L8Query{})
	node, _ := vnic.Resources().Introspector().Inspect(sla.ServiceItem())
	introspecting.AddPrimaryKeyDecorator(node, sla.PrimaryKeys()...)
	//b, e := vnic.Resources().Services().Activate(sla, vnic)
	//bs := b.(*BaseService)
	//go recovery.RecoveryCheck(sla.ServiceName(), sla.ServiceArea(), bs.cache, vnic)
	return nil
}

func (this *BaseService) Activate(sla *ifs.ServiceLevelAgreement, vnic ifs.IVNic) error {
	this.sla = sla
	this.nQueue = queues.NewQueue(sla.ServiceName(), 10000)
	this.running = true
	if this.sla.Stateful() {
		this.cache = cache.NewCache(this.sla.ServiceItem(), this.sla.InitItems(),
			this.sla.Store(), vnic.Resources())
		if sla.MetadataFunc() != nil {
			for name, f := range sla.MetadataFunc() {
				this.cache.AddMetadataFunc(name, f)
			}
		}
	}
	this.cache.SetNotificationsFor(sla.ServiceName(), sla.ServiceArea())
	this.vnic = vnic

	go this.processNotificationQueue()
	return nil
}

func (this *BaseService) DeActivate() error {
	this.Shutdown()
	return nil
}

func (this *BaseService) validateElem(pb ifs.IElements) error {
	v := reflect.ValueOf(pb.Element())
	if !v.IsValid() {
		return errors.New("Invalid element ")
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Type().Name() != this.cache.ModelType() {
		return errors.New("Invalid element type " + v.Type().Name())
	}
	return nil
}
