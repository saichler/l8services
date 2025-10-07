package manager

import (
	"fmt"
	"sync"

	"github.com/saichler/l8types/go/ifs"
)

type participantSet struct {
	uuids map[string]struct{}
	mtx   sync.RWMutex
}

type ParticipantRegistry struct {
	participants sync.Map // key: serviceKey string -> *participantSet
}

func NewParticipantRegistry() *ParticipantRegistry {
	return &ParticipantRegistry{}
}

func (pr *ParticipantRegistry) handleRegistry(action ifs.Action, vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	localUuid := vnic.Resources().SysConfig().LocalUuid
	fmt.Println("[PARTICIPANT]", localUuid, "- handleRegistry called with action:", action, "for", msg.ServiceName(), "area", msg.ServiceArea())
	switch action {
	case ifs.ServiceRegister:
		return pr.handleServiceRegister(vnic, msg)
	case ifs.ServiceUnregister:
		return pr.handleServiceUnregister(vnic, msg)
	case ifs.ServiceQuery:
		return pr.handleServiceQuery(vnic, msg)
	}
	return nil
}

func (pr *ParticipantRegistry) handleServiceRegister(vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	localUuid := vnic.Resources().SysConfig().LocalUuid
	key := makeServiceKey(msg.ServiceName(), msg.ServiceArea())
	ps := pr.getOrCreateParticipantSet(key)

	fmt.Println("[PARTICIPANT]", localUuid, "- Registering participant", msg.Source(), "for", msg.ServiceName(), "area", msg.ServiceArea())
	vnic.Resources().Logger().Debug("Registering participant", msg.Source(), "for", msg.ServiceName(), "area", msg.ServiceArea())

	ps.mtx.Lock()
	ps.uuids[msg.Source()] = struct{}{}
	ps.mtx.Unlock()

	vnic.Resources().Logger().Info("Registered participant", msg.Source(), "for", msg.ServiceName(), "area", msg.ServiceArea())
	return nil
}

func (pr *ParticipantRegistry) handleServiceUnregister(vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	localUuid := vnic.Resources().SysConfig().LocalUuid
	key := makeServiceKey(msg.ServiceName(), msg.ServiceArea())
	ps := pr.getParticipantSet(key)
	if ps == nil {
		fmt.Println("[PARTICIPANT]", localUuid, "- No participant set for", msg.ServiceName(), "area", msg.ServiceArea())
		return nil
	}

	fmt.Println("[PARTICIPANT]", localUuid, "- Unregistering participant", msg.Source(), "for", msg.ServiceName(), "area", msg.ServiceArea())
	vnic.Resources().Logger().Debug("Unregistering participant", msg.Source(), "for", msg.ServiceName(), "area", msg.ServiceArea())

	ps.mtx.Lock()
	delete(ps.uuids, msg.Source())
	ps.mtx.Unlock()

	vnic.Resources().Logger().Info("Unregistered participant", msg.Source(), "for", msg.ServiceName(), "area", msg.ServiceArea())
	return nil
}

func (pr *ParticipantRegistry) handleServiceQuery(vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	localUuid := vnic.Resources().SysConfig().LocalUuid
	key := makeServiceKey(msg.ServiceName(), msg.ServiceArea())
	ps := pr.getParticipantSet(key)

	fmt.Println("[PARTICIPANT]", localUuid, "- Service query from", msg.Source(), "for", msg.ServiceName(), "area", msg.ServiceArea())
	vnic.Resources().Logger().Debug("Service query from", msg.Source(), "for", msg.ServiceName(), "area", msg.ServiceArea())

	if ps != nil {
		ps.mtx.RLock()
		_, isParticipant := ps.uuids[localUuid]
		ps.mtx.RUnlock()

		if isParticipant {
			fmt.Println("[PARTICIPANT]", localUuid, "- Responding to query, I am a participant")
			vnic.Resources().Logger().Debug("Responding to query, I am a participant")
			// Respond that we are a participant
			vnic.Unicast(msg.Source(), msg.ServiceName(), msg.ServiceArea(), ifs.ServiceRegister, nil)
		}
	}

	return nil
}

func (pr *ParticipantRegistry) RegisterParticipant(serviceName string, serviceArea byte, uuid string) {
	key := makeServiceKey(serviceName, serviceArea)
	ps := pr.getOrCreateParticipantSet(key)

	ps.mtx.Lock()
	ps.uuids[uuid] = struct{}{}
	ps.mtx.Unlock()
}

func (pr *ParticipantRegistry) UnregisterParticipant(serviceName string, serviceArea byte, uuid string) {
	key := makeServiceKey(serviceName, serviceArea)
	ps := pr.getParticipantSet(key)
	if ps == nil {
		return
	}

	ps.mtx.Lock()
	delete(ps.uuids, uuid)
	ps.mtx.Unlock()
}

func (pr *ParticipantRegistry) GetParticipants(serviceName string, serviceArea byte) map[string]bool {
	key := makeServiceKey(serviceName, serviceArea)
	ps := pr.getParticipantSet(key)
	if ps == nil {
		fmt.Println("[PARTICIPANT] GetParticipants: No participant set for", serviceName, "area", serviceArea)
		return map[string]bool{}
	}

	ps.mtx.RLock()
	defer ps.mtx.RUnlock()

	participants := make(map[string]bool, len(ps.uuids))
	for uuid := range ps.uuids {
		participants[uuid] = true
	}

	fmt.Println("[PARTICIPANT] GetParticipants:", serviceName, "area", serviceArea, "count:", len(participants), "participants:", participants)

	return participants
}

func (pr *ParticipantRegistry) IsParticipant(serviceName string, serviceArea byte, uuid string) bool {
	key := makeServiceKey(serviceName, serviceArea)
	ps := pr.getParticipantSet(key)
	if ps == nil {
		return false
	}

	ps.mtx.RLock()
	defer ps.mtx.RUnlock()

	_, exists := ps.uuids[uuid]
	return exists
}

func (pr *ParticipantRegistry) ParticipantCount(serviceName string, serviceArea byte) int {
	key := makeServiceKey(serviceName, serviceArea)
	ps := pr.getParticipantSet(key)
	if ps == nil {
		return 0
	}

	ps.mtx.RLock()
	defer ps.mtx.RUnlock()

	return len(ps.uuids)
}

func (pr *ParticipantRegistry) UnregisterParticipantFromAll(uuid string) {
	fmt.Println("[PARTICIPANT] Unregistering", uuid, "from all services")
	pr.participants.Range(func(key, value interface{}) bool {
		ps := value.(*participantSet)
		ps.mtx.Lock()
		delete(ps.uuids, uuid)
		ps.mtx.Unlock()
		fmt.Println("[PARTICIPANT] Removed", uuid, "from service", key)
		return true
	})
}

func (pr *ParticipantRegistry) getParticipantSet(key string) *participantSet {
	ps, ok := pr.participants.Load(key)
	if !ok {
		return nil
	}
	return ps.(*participantSet)
}

func (pr *ParticipantRegistry) getOrCreateParticipantSet(key string) *participantSet {
	ps, ok := pr.participants.Load(key)
	if ok {
		return ps.(*participantSet)
	}

	newPs := &participantSet{
		uuids: make(map[string]struct{}),
	}
	pr.participants.Store(key, newPs)
	return newPs
}
