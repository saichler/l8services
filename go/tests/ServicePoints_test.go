package tests

import (
	. "github.com/saichler/shared/go/tests/infra"
	"github.com/saichler/types/go/testtypes"
	"github.com/saichler/types/go/types"
	"testing"
)

func TestServicePoints(t *testing.T) {
	testsp := NewTestServicePointHandler("TestProto")
	pb := &testtypes.TestProto{}
	err := globals.ServicePoints().RegisterServicePoint(0, nil, testsp)
	if err == nil {
		Log.Fail("Expected an error")
		return
	}
	err = globals.ServicePoints().RegisterServicePoint(0, pb, nil)
	if err == nil {
		Log.Fail("Expected an error")
		return
	}
	err = globals.ServicePoints().RegisterServicePoint(0, pb, testsp)
	if err != nil {
		Log.Fail(t, err)
		return
	}
	sp, ok := globals.ServicePoints().ServicePointHandler("TestProto")
	if !ok {
		Log.Fail(t, "Service Point Not Found")
		return
	}
	sp.Topic()
	globals.ServicePoints().Handle(pb, types.Action_POST, nil, nil, false)
	globals.ServicePoints().Handle(pb, types.Action_PUT, nil, nil, false)
	globals.ServicePoints().Handle(pb, types.Action_DELETE, nil, nil, false)
	globals.ServicePoints().Handle(pb, types.Action_GET, nil, nil, false)
	globals.ServicePoints().Handle(pb, types.Action_PATCH, nil, nil, false)
	globals.ServicePoints().Handle(pb, types.Action_Invalid_Action, nil, nil, false)
	msg := &types.Message{}
	msg.FailMsg = "The failed message"
	msg.SourceUuid = "The source uuid"
	globals.ServicePoints().Handle(pb, types.Action_POST, nil, msg, false)
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
	}
}
