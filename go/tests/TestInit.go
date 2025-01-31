package tests

import (
	inspect2 "github.com/saichler/reflect/go/reflect/inspect"
	"github.com/saichler/servicepoints/go/points/service_points"
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/share/logger"
	"github.com/saichler/shared/go/share/registry"
	"github.com/saichler/shared/go/share/resources"
	"github.com/saichler/shared/go/share/shallow_security"
	"github.com/saichler/shared/go/tests/infra"
	"github.com/saichler/shared/go/types"
)

var globals interfaces.IResources
var log interfaces.ILogger

func init() {
	log = logger.NewLoggerDirectImpl(&logger.FmtLogMethod{})
	infra.Log = log
	registry := registry.NewRegistry()
	config := &types.VNicConfig{MaxDataSize: resources.DEFAULT_MAX_DATA_SIZE,
		RxQueueSize: resources.DEFAULT_QUEUE_SIZE,
		TxQueueSize: resources.DEFAULT_QUEUE_SIZE,
		LocalAlias:  "servicepointstest",
		Topics:      map[string]bool{}}
	security := shallow_security.CreateShallowSecurityProvider()
	inspect := inspect2.NewIntrospect(registry)
	sps := service_points.NewServicePoints(inspect, config)
	globals = resources.NewResources(registry, security, sps, log, nil, nil, config, inspect)
}
