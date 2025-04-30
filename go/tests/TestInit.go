package tests

import (
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_topology"
	"github.com/saichler/layer8/go/overlay/protocol"
	. "github.com/saichler/types/go/common"
)

var topo *TestTopology
var globals, alias = CreateResources(50000, 1, Trace_Level)

func init() {
	Log.SetLogLevel(Trace_Level)
}

func setup() {
	setupTopology()
}

func tear() {
	shutdownTopology()
}

func reset(name string) {
	Log.Info("*** ", name, " end ***")
	topo.ResetHandlers()
	Log.SetLogLevel(Trace_Level)
}

func setupTopology() {
	protocol.CountMessages = true
	topo = NewTestTopology(4, []int{20000, 30000, 40000}, Info_Level)
}

func shutdownTopology() {
	topo.Shutdown()
}
