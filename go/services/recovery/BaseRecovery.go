package recovery

import (
	"reflect"
	"time"

	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8utils/go/utils/cache"
	"github.com/saichler/l8utils/go/utils/strings"
)

func RecoveryCheck(serviceName string, serviceArea byte, cache *cache.Cache, nic ifs.IVNic) {
	time.Sleep(time.Second * 5)
	leader := nic.Resources().Services().GetLeader(serviceName, serviceArea)
	if nic.Resources().SysConfig().LocalUuid == leader {
		nic.Resources().Logger().Debug("Recover was called on leader, ignoring")
		return
	}
	gsql := "select * from " + cache.ModelType() + " limit 1 page 0"
	resp := nic.Request(leader, serviceName, serviceArea, ifs.GET, gsql, 5)
	if resp == nil {
		return
	}
	if resp.Error() != nil {
		nic.Resources().Logger().Error("Recover: ", resp.Error().Error())
		return
	}

	list, _ := resp.AsList(nic.Resources().Registry())
	if list == nil {
		return
	}
	v := reflect.ValueOf(list)
	v = v.Elem()
	stats := v.FieldByName("Stats")
	if stats.IsValid() && stats.Kind() == reflect.Map {
		total := stats.MapIndex(reflect.ValueOf("Total"))
		if total.IsValid() {
			t := int(total.Int())
			if t != cache.Size() {
				nic.Resources().Logger().Error("Synching: ", serviceName, " area ", serviceArea,
					" local is ", cache.Size(), " total should be ", t)
				Sync(serviceName, serviceArea, cache, nic, t)
			}
		}
	}
}

func Sync(serviceName string, serviceArea byte, cache *cache.Cache, nic ifs.IVNic, total int) {
	leader := nic.Resources().Services().GetLeader(serviceName, serviceArea)
	handler, _ := nic.Resources().Services().ServiceHandler(serviceName, serviceArea)
	pages := total / 50
	if total%50 != 0 {
		pages++
	}

	gsql := "select * from " + cache.ModelType() + " limit 50 page "
	for i := 0; i <= pages; i++ {
		qr := strings.New(gsql, i).String()
		resp := nic.Request(leader, serviceName, serviceArea, ifs.GET, qr, 15)
		if resp == nil {
			nic.Resources().Logger().Error("Sync: ", serviceName, " area ", serviceArea, " nil Response for page ", i)
			break
		}
		if resp.Error() != nil {
			nic.Resources().Logger().Error("Sync: ", serviceName, " area ", serviceArea,
				" error Response for page ", i, " ", resp.Error())
			break
		}
		resp = object.NewNotify(resp.Elements())
		handler.Post(resp, nic)
	}
}
