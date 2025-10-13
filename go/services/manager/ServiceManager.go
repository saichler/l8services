package manager

import (
	"bytes"
	"strconv"

	"github.com/saichler/l8bus/go/overlay/health"
	"github.com/saichler/l8services/go/services/replication"
	"github.com/saichler/l8services/go/services/transaction/states"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types/l8health"
	"github.com/saichler/l8types/go/types/l8notify"
	"github.com/saichler/l8types/go/types/l8services"
	"github.com/saichler/l8utils/go/utils/maps"
	"github.com/saichler/l8utils/go/utils/notify"
)

type ServiceManager struct {
	services            *ServicesMap
	trManager           *states.TransactionManager
	distributedCaches   *maps.SyncMap
	resources           ifs.IResources
	leaderElection      *LeaderElection
	participantRegistry *ParticipantRegistry
}

func NewServices(resources ifs.IResources) ifs.IServices {
	sp := &ServiceManager{}
	sp.services = NewServicesMap()
	sp.resources = resources
	sp.trManager = states.NewTransactionManager(sp)
	sp.distributedCaches = maps.NewSyncMap()
	sp.leaderElection = NewLeaderElection()
	sp.participantRegistry = NewParticipantRegistry()
	_, err := sp.resources.Registry().Register(&l8notify.L8NotificationSet{})
	if err != nil {
		panic(err)
	}
	sp.resources.Registry().Register(&l8services.L8Transaction{})
	sp.resources.Registry().Register(&replication.ReplicationService{})
	return sp
}

func (this *ServiceManager) RegisterServiceHandlerType(handler ifs.IServiceHandler) {
	this.resources.Registry().Register(handler)
}

func (this *ServiceManager) Handle(pb ifs.IElements, action ifs.Action, vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	if vnic == nil {
		return object.NewError("Handle: vnic cannot be nil")
	}
	if msg == nil {
		return object.NewError("Handle: message cannot be nil")
	}
	err := vnic.Resources().Security().CanDoAction(action, pb, vnic.Resources().SysConfig().LocalUuid, "")
	if err != nil {
		return object.NewError(err.Error())
	}

	// Handle participant registry actions
	if action >= ifs.ServiceRegister && action <= ifs.ServiceQuery {
		vnic.Resources().Logger().Debug("Routing to participant registry, action:", action)
		return this.participantRegistry.handleRegistry(action, vnic, msg)
	}

	// Handle leader election actions
	if action >= ifs.ElectionRequest && action <= ifs.LeaderChallenge {
		return this.leaderElection.handleElection(action, vnic, msg)
	}

	if msg.Action() == ifs.Sync {
		key := cacheKey(msg.ServiceName(), msg.ServiceArea())
		cache, ok := this.distributedCaches.Get(key)
		if ok {
			go cache.(ifs.IDistributedCache).Sync()
		}
		return nil
	}

	if msg.Action() == ifs.EndPoints {
		this.sendEndPoints(vnic)
		return nil
	}

	h, ok := this.services.get(msg.ServiceName(), msg.ServiceArea())
	if !ok {
		hp := health.Health(vnic.Resources()).Health(vnic.Resources().SysConfig().LocalUuid)
		alias := "Unknown"
		if hp != nil {
			alias = hp.Alias
		}
		return object.NewError(alias + " - Cannot find active handler for service " + msg.ServiceName() +
			" area " + strconv.Itoa(int(msg.ServiceArea())))
	}

	if msg.FailMessage() != "" {
		return h.Failed(pb, vnic, msg)
	}

	isStartTransaction := h.TransactionConfig() != nil && msg.Action() < ifs.ElectionRequest && this.GetLeader(msg.ServiceName(), msg.ServiceArea()) != ""
	if isStartTransaction {
		if msg.Tr_State() == ifs.NotATransaction {
			vnic.Resources().Logger().Debug("Starting transaction")
			defer vnic.Resources().Logger().Debug("Defer Starting transaction")
			return this.trManager.Create(msg, vnic)
		}
		vnic.Resources().Logger().Debug("Running transaction")
		defer vnic.Resources().Logger().Debug("Defer Running transaction")
		return this.trManager.Run(msg, vnic)
	}

	return this.handle(h, pb, action, vnic)
}

func (this *ServiceManager) updateReplicationIndex(serviceName string, serviceArea byte, key string, replica byte, r ifs.IResources) {
	index := replication.ReplicationIndex(serviceName, serviceArea, r)
	if index.Keys == nil {
		index.Keys = make(map[string]*l8services.L8ReplicationKey)
	}
	if index.Keys[key] == nil {
		index.Keys[key] = &l8services.L8ReplicationKey{}
		index.Keys[key].Location = make(map[string]int32)
	}
	index.Keys[key].Location[r.SysConfig().LocalUuid] = int32(replica)
	repService := replication.Service(r)
	repService.Patch(object.New(nil, index), nil)
}

func (this *ServiceManager) TransactionHandle(pb ifs.IElements, action ifs.Action, vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	this.resources.Logger().Info("Transaction Handle:", msg.ServiceName(), ",", msg.ServiceArea(), ",", action)
	h, _ := this.services.get(msg.ServiceName(), msg.ServiceArea())
	if h == nil {
		this.resources.Logger().Info("Transaction Handle: No handler for service "+msg.ServiceName(), "-", msg.ServiceArea())
	}
	resp := this.handle(h, pb, action, vnic)
	if resp.Error() == nil && h.TransactionConfig().Replication() {
		key := h.TransactionConfig().KeyOf(pb, vnic.Resources())
		this.updateReplicationIndex(msg.ServiceName(), msg.ServiceArea(), key, msg.Tr_Replica(), vnic.Resources())
	}
	return resp
}

func (this *ServiceManager) handle(h ifs.IServiceHandler, pb ifs.IElements, action ifs.Action, vnic ifs.IVNic) ifs.IElements {

	if h == nil {
		return object.New(nil, pb)
	}

	switch action {
	case ifs.POST:
		return h.Post(pb, vnic)
	case ifs.PUT:
		return h.Put(pb, vnic)
	case ifs.PATCH:
		return h.Patch(pb, vnic)
	case ifs.DELETE:
		return h.Delete(pb, vnic)
	case ifs.GET:
		return h.Get(pb, vnic)
	default:
		return object.NewError("invalid action, ignoring")
	}
}

func (this *ServiceManager) Notify(pb ifs.IElements, vnic ifs.IVNic, msg *ifs.Message, isTransaction bool) ifs.IElements {
	if vnic.Resources().SysConfig().LocalUuid == msg.Source() {
		return object.New(nil, nil)
	}
	notification := pb.Element().(*l8notify.L8NotificationSet)
	h, ok := this.services.get(notification.ServiceName, byte(notification.ServiceArea))
	if !ok {
		return object.NewError("Cannot find active handler for service " + msg.ServiceName() +
			" area " + strconv.Itoa(int(msg.ServiceArea())))
	}

	if msg != nil && msg.FailMessage() != "" {
		return h.Failed(pb, vnic, msg)
	}
	item, err := notify.ItemOf(notification, this.resources)
	if err != nil {
		return object.NewError(err.Error())
	}
	npb := object.NewNotify(item)

	switch notification.Type {
	case l8notify.L8NotificationType_Add:
		return h.Post(npb, vnic)
	case l8notify.L8NotificationType_Replace:
		return h.Put(npb, vnic)
	case l8notify.L8NotificationType_Sync:
		fallthrough
	case l8notify.L8NotificationType_Update:
		return h.Patch(npb, vnic)
	case l8notify.L8NotificationType_Delete:
		result := h.Delete(npb, vnic)
		if notification.ServiceName == health.ServiceName {
			this.onNodeDelete(item.(*l8health.L8Health).AUuid)
		}
		return result
	default:
		return object.NewError("invalid notification type, ignoring")
	}
}

func (this *ServiceManager) onNodeDelete(uuid string) {
	this.participantRegistry.UnregisterParticipantFromAll(uuid)
	this.resources.Logger().Info("Unregistered all services for failed node", uuid)
}

func (this *ServiceManager) ServiceHandler(serviceName string, serviceArea byte) (ifs.IServiceHandler, bool) {
	return this.services.get(serviceName, serviceArea)
}

func (this *ServiceManager) RegisterDistributedCache(cache ifs.IDistributedCache) {
	key := cacheKey(cache.ServiceName(), cache.ServiceArea())
	this.distributedCaches.Put(key, cache)
}

func (this *ServiceManager) sendEndPoints(vnic ifs.IVNic) {
	webServices := this.services.webServices()
	for _, ws := range webServices {
		vnic.Resources().Logger().Info("Sent Webservice multicast for ", ws.ServiceName(), " area ", ws.ServiceArea())
		vnic.Multicast(ifs.WebService, 0, ifs.POST, ws.Serialize())
	}
}

func cacheKey(serviceName string, serviceArea byte) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}

func (this *ServiceManager) Services() *l8services.L8Services {
	return this.services.serviceList()
}

func (this *ServiceManager) StartElection(serviceName string, serviceArea byte, vnic ifs.IVNic) {
	this.leaderElection.StartElectionForService(serviceName, serviceArea, vnic)
}

func (this *ServiceManager) GetLeader(serviceName string, serviceArea byte) string {
	return this.leaderElection.GetLeader(serviceName, serviceArea)
}

func (this *ServiceManager) IsLeader(serviceName string, serviceArea byte, uuid string) bool {
	return this.leaderElection.IsLeader(serviceName, serviceArea, uuid)
}

func (this *ServiceManager) RegisterParticipant(serviceName string, serviceArea byte, uuid string) {
	this.participantRegistry.RegisterParticipant(serviceName, serviceArea, uuid)
}

func (this *ServiceManager) UnregisterParticipant(serviceName string, serviceArea byte, uuid string) {
	this.participantRegistry.UnregisterParticipant(serviceName, serviceArea, uuid)
}

func (this *ServiceManager) GetParticipants(serviceName string, serviceArea byte) map[string]byte {
	return this.participantRegistry.GetParticipants(serviceName, serviceArea)
}

func (this *ServiceManager) IsParticipant(serviceName string, serviceArea byte, uuid string) bool {
	return this.participantRegistry.IsParticipant(serviceName, serviceArea, uuid)
}

func (this *ServiceManager) ParticipantCount(serviceName string, serviceArea byte) int {
	return this.participantRegistry.ParticipantCount(serviceName, serviceArea)
}

func (this *ServiceManager) RoundRobinParticipants(serviceName string, serviceArea byte, replications int) map[string]byte {
	return this.participantRegistry.RoundRobinParticipants(serviceName, serviceArea, replications)
}
