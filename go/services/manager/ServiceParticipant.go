package manager

import (
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
	key := makeServiceKey(msg.ServiceName(), msg.ServiceArea())
	ps := pr.getOrCreateParticipantSet(key)

	ps.mtx.Lock()
	ps.uuids[msg.Source()] = struct{}{}
	ps.mtx.Unlock()

	vnic.Resources().Logger().Info("Registered participant", msg.Source(), "for", msg.ServiceName(), "area", msg.ServiceArea())
	return nil
}

func (pr *ParticipantRegistry) handleServiceUnregister(vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	key := makeServiceKey(msg.ServiceName(), msg.ServiceArea())
	ps := pr.getParticipantSet(key)
	if ps == nil {
		return nil
	}

	ps.mtx.Lock()
	delete(ps.uuids, msg.Source())
	ps.mtx.Unlock()

	vnic.Resources().Logger().Info("Unregistered participant", msg.Source(), "for", msg.ServiceName(), "area", msg.ServiceArea())
	return nil
}

func (pr *ParticipantRegistry) handleServiceQuery(vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	key := makeServiceKey(msg.ServiceName(), msg.ServiceArea())
	ps := pr.getParticipantSet(key)

	localUuid := vnic.Resources().SysConfig().LocalUuid

	if ps != nil {
		ps.mtx.RLock()
		_, isParticipant := ps.uuids[localUuid]
		ps.mtx.RUnlock()

		if isParticipant {
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

func (pr *ParticipantRegistry) GetParticipants(serviceName string, serviceArea byte) []string {
	key := makeServiceKey(serviceName, serviceArea)
	ps := pr.getParticipantSet(key)
	if ps == nil {
		return []string{}
	}

	ps.mtx.RLock()
	defer ps.mtx.RUnlock()

	participants := make([]string, 0, len(ps.uuids))
	for uuid := range ps.uuids {
		participants = append(participants, uuid)
	}

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

