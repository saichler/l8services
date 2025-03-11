package tests

import (
	"github.com/saichler/layer8/go/overlay/protocol"
	"github.com/saichler/layer8/go/overlay/vnet"
	vnic2 "github.com/saichler/layer8/go/overlay/vnic"
	"github.com/saichler/reflect/go/reflect/introspecting"
	"github.com/saichler/servicepoints/go/points/service_points"
	"github.com/saichler/shared/go/share/logger"
	"github.com/saichler/shared/go/share/registry"
	"github.com/saichler/shared/go/share/resources"
	"github.com/saichler/shared/go/tests/infra"
	"github.com/saichler/types/go/common"
	. "github.com/saichler/types/go/common"
	"github.com/saichler/types/go/testtypes"
	"github.com/saichler/types/go/types"
	"time"
)

var log = logger.NewLoggerDirectImpl(&logger.FmtLogMethod{})
var globals IResources
var sw1 *vnet.VNet
var sw2 *vnet.VNet
var eg1 IVirtualNetworkInterface
var eg2 IVirtualNetworkInterface
var eg3 IVirtualNetworkInterface
var eg4 IVirtualNetworkInterface
var eg5 IVirtualNetworkInterface
var tsps = make(map[string]*infra.TestServicePointHandler)

func init() {
	log.SetLogLevel(Trace_Level)
	protocol.UsingContainers = false
	initGlobals()
	infra.Log = log
}

func initGlobals() {
	registry := registry.NewRegistry()
	config := &types.VNicConfig{MaxDataSize: resources.DEFAULT_MAX_DATA_SIZE,
		RxQueueSize: resources.DEFAULT_QUEUE_SIZE,
		TxQueueSize: resources.DEFAULT_QUEUE_SIZE,
		LocalAlias:  "servicepointstest"}
	secure, err := common.LoadSecurityProvider("security.so")
	if err != nil {
		panic(err)
	}
	inspector := introspecting.NewIntrospect(registry)
	sps := service_points.NewServicePoints(inspector, config)
	globals = resources.NewResources(registry, secure, sps, log, nil, nil, config, inspector)
}

func setup() {
	setupTopology()
}

func tear() {
	shutdownTopology()
}

func reset(name string) {
	log.Info("*** ", name, " end ***")
	for _, t := range tsps {
		t.PostNumber = 0
		t.DeleteNumber = 0
		t.PutNumber = 0
		t.PatchNumber = 0
		t.GetNumber = 0
		t.FailedNumber = 0
		t.ErrorMode = false
		t.Tr = false
	}
	time.Sleep(time.Second)
}

func setupTopology() {
	sw1 = createSwitch(50000, "sw1")
	sw2 = createSwitch(50001, "sw2")
	sleep()
	eg1 = createEdge(50000, "eg1", true)
	eg2 = createEdge(50000, "eg2", true)
	eg3 = createEdge(50001, "eg3", true)
	eg4 = createEdge(50001, "eg4", true)
	eg5 = createEdge(50000, "eg5", false)
	sleep()
	connectSwitches(sw1, sw2)
	time.Sleep(time.Second)
}

func shutdownTopology() {
	eg4.Shutdown()
	eg3.Shutdown()
	eg2.Shutdown()
	eg1.Shutdown()
	sw2.Shutdown()
	sw1.Shutdown()
	sleep()
}

func createSwitch(port uint32, name string) *vnet.VNet {
	reg := registry.NewRegistry()
	secure, err := common.LoadSecurityProvider("security.so")
	if err != nil {
		panic("")
	}
	config := &types.VNicConfig{MaxDataSize: resources.DEFAULT_MAX_DATA_SIZE,
		RxQueueSize: resources.DEFAULT_QUEUE_SIZE,
		TxQueueSize: resources.DEFAULT_QUEUE_SIZE,
		LocalAlias:  name}
	ins := introspecting.NewIntrospect(reg)
	sps := service_points.NewServicePoints(ins, config)

	res := resources.NewResources(reg, secure, sps, log, nil, nil, config, ins)
	res.Config().VnetPort = port
	sw := vnet.NewVNet(res)
	sw.Start()
	return sw
}

func createEdge(port uint32, name string, addTestTopic bool) IVirtualNetworkInterface {
	reg := registry.NewRegistry()
	secure, err := common.LoadSecurityProvider("security.so")
	if err != nil {
		panic(err)
	}
	config := &types.VNicConfig{MaxDataSize: resources.DEFAULT_MAX_DATA_SIZE,
		RxQueueSize:              resources.DEFAULT_QUEUE_SIZE,
		TxQueueSize:              resources.DEFAULT_QUEUE_SIZE,
		LocalAlias:               name,
		KeepAliveIntervalSeconds: 2}
	ins := introspecting.NewIntrospect(reg)
	sps := service_points.NewServicePoints(ins, config)

	resourcs := resources.NewResources(reg, secure, sps, log, nil, nil, config, ins)
	resourcs.Config().VnetPort = port
	tsps[name] = infra.NewTestServicePointHandler(name)

	if addTestTopic {
		sp := resourcs.ServicePoints()
		err := sp.RegisterServicePoint(0, &testtypes.TestProto{}, tsps[name])
		if err != nil {
			panic(err)
		}
	}

	vnic := vnic2.NewVirtualNetworkInterface(resourcs, nil)
	vnic.Start()

	return vnic
}

func connectSwitches(s1, s2 *vnet.VNet) {
	s1.ConnectNetworks("127.0.0.1", s2.Resources().Config().VnetPort)
}

func sleep() {
	time.Sleep(time.Millisecond * 100)
}
