package tests

import (
	"github.com/saichler/shared/go/tests"
	"github.com/saichler/shared/go/tests/infra"
	"github.com/saichler/types/go/types"
	"testing"
)

func TestServicePoints(t *testing.T) {
	testsp := infra.NewTestServicePointHandler("testsp")
	pb := &tests.TestProto{}
	err := globals.ServicePoints().RegisterServicePoint(0, nil, testsp)
	if err == nil {
		log.Fail("Expected an error")
		return
	}
	err = globals.ServicePoints().RegisterServicePoint(0, pb, nil)
	if err == nil {
		log.Fail("Expected an error")
		return
	}
	err = globals.ServicePoints().RegisterServicePoint(0, pb, testsp)
	if err != nil {
		log.Fail(t, err)
		return
	}
	sp, ok := globals.ServicePoints().ServicePointHandler("TestProto")
	if !ok {
		log.Fail(t, "Service Point Not Found")
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
	if testsp.PostNumber != 1 {
		log.Fail(t, "Post is not 1")
	}
	if testsp.PutNumber != 1 {
		log.Fail(t, "Put is not 1")
	}
	if testsp.DeleteNumber != 1 {
		log.Fail(t, "Delete is not 1")
	}
	if testsp.PatchNumber != 1 {
		log.Fail(t, "Patch is not 1")
	}
	if testsp.GetNumber != 1 {
		log.Fail(t, "Get is not 1")
	}
}
