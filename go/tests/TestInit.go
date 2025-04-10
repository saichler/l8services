package tests

import (
	. "github.com/saichler/l8test/go/infra/t_resources"
	. "github.com/saichler/l8test/go/infra/t_topology"
	. "github.com/saichler/types/go/common"
)

var topo *TestTopology
var globals, alias = CreateResources(50000, 1)

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
	topo = NewTestTopology(4, 20000, 30000, 40000)
}

func shutdownTopology() {
	topo.Shutdown()
}

func setTransactionMode(replicationCount int) {
	handlers := topo.AllHandlers()
	for _, ts := range handlers {
		ts.SetTr(true)
		ts.SetReplicationCount(replicationCount)
	}
}
