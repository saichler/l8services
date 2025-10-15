package base

import (
	"errors"
	"reflect"

	"github.com/saichler/l8reflect/go/reflect/introspecting"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8api"
	"github.com/saichler/l8types/go/types/l8web"
	"github.com/saichler/l8utils/go/utils/cache"
)

func Activate(serviceConfig *ifs.ServiceConfig, vnic ifs.IVNic) error {
	vnic.Resources().Registry().Register(&BaseService{})
	vnic.Resources().Registry().Register(serviceConfig.ServiceItemList)
	vnic.Resources().Registry().Register(&l8web.L8Empty{})
	vnic.Resources().Registry().Register(&l8api.L8Query{})
	node, _ := vnic.Resources().Introspector().Inspect(serviceConfig.ServiceItem)
	introspecting.AddPrimaryKeyDecorator(node, serviceConfig.PrimaryKey...)
	_, e := vnic.Resources().Services().Activate("BaseService", serviceConfig.ServiceName, serviceConfig.ServiceArea,
		vnic.Resources(), vnic, serviceConfig)
	return e
}

func (this *BaseService) Activate(serviceName string, serviceArea byte,
	resources ifs.IResources, listener ifs.IServiceCacheListener, args ...interface{}) error {
	this.serviceConfig = args[0].(*ifs.ServiceConfig)
	this.cache = cache.NewCache(this.serviceConfig.ServiceItem, this.serviceConfig.InitItems,
		this.serviceConfig.Store, resources)
	this.cache.SetNotificationsFor(serviceName, serviceArea)
	this.vnic = listener.(ifs.IVNic)
	return nil
}

func (this *BaseService) DeActivate() error {
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
