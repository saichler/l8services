package tests

import (
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_servicepoints"
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/testtypes"
	"testing"
)

func TestServicePoints(t *testing.T) {
	testsp := &TestServicePointHandler{}
	pb := object.New(nil, &testtypes.TestProto{})
	globals.ServicePoints().AddServicePointType(testsp)
	_, err := globals.ServicePoints().Activate("", "", 0, nil, nil)
	if err == nil {
		Log.Fail("Expected an error")
		return
	}
	_, err = globals.ServicePoints().Activate("TestServicePointHandler", "", 0, nil, nil)
	if err == nil {
		Log.Fail("Expected an error")
		return
	}
	_, err = globals.ServicePoints().Activate(ServicePointType, ServiceName, 0, nil, nil, "")
	if err != nil {
		Log.Fail(t, err)
		return
	}
	sp, ok := globals.ServicePoints().ServicePointHandler(ServiceName, 0)
	if !ok {
		Log.Fail(t, "Service Point Not Found")
		return
	}
	sp.TransactionMethod()

	globals.ServicePoints().Handle(pb, ifs.POST, nil, nil)
	globals.ServicePoints().Handle(pb, ifs.PUT, nil, nil)
	globals.ServicePoints().Handle(pb, ifs.DELETE, nil, nil)
	globals.ServicePoints().Handle(pb, ifs.GET, nil, nil)
	globals.ServicePoints().Handle(pb, ifs.PATCH, nil, nil)

	/*
		msg := &protocol.Message{}
		msg.Set "The failed message"
		msg.Source = "The source uuid"
		globals.ServicePoints().Handle(pb, ifs.POST, nil, msg, false)
		if testsp.PostN() != 1 {
			Log.Fail(t, "Post is not 1")
		}
		if testsp.PutN() != 1 {
			Log.Fail(t, "Put is not 1")
		}
		if testsp.DeleteN() != 1 {
			Log.Fail(t, "Delete is not 1")
		}
		if testsp.PatchN() != 1 {
			Log.Fail(t, "Patch is not 1")
		}
		if testsp.GetN() != 1 {
			Log.Fail(t, "Get is not 1")
		}*/
}
