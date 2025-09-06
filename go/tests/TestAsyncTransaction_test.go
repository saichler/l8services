package tests

import (
	"testing"

	"github.com/saichler/l8types/go/ifs"
)

func TestAsyncTransaction(t *testing.T) {
	topo.SetLogLevel(ifs.Debug_Level)
	defer reset("TestTransaction")

	eg2_2 := topo.VnicByVnetNum(2, 2)
	eg1_1 := topo.VnicByVnetNum(1, 1)

	if !doAsyncTransaction(ifs.POST, eg2_2, 1, t, true) {
		return
	}

	if !doAsyncTransaction(ifs.POST, eg2_2, 2, t, true) {
		return
	}

	if !doAsyncTransaction(ifs.POST, eg1_1, 3, t, true) {
		return
	}

}
