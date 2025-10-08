package manager

import (
	"bytes"
	"strconv"
	"sync"
	"time"

	"github.com/saichler/l8types/go/ifs"
)

const (
	electionTimeout  = 3 * time.Second
	heartbeatTimeout = 5 * time.Second
	heartbeatPeriod  = 2 * time.Second
)

type electionState byte

const (
	idle      electionState = 0
	electing  electionState = 1
	isLeader  electionState = 2
	hasLeader electionState = 3
)

type leaderInfo struct {
	leaderUuid      string
	lastHeartbeat   time.Time
	state           electionState
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
	stopChan        chan struct{}
	mtx             sync.RWMutex
}

type LeaderElection struct {
	leaders sync.Map // key: serviceKey string -> *leaderInfo
}

func NewLeaderElection() *LeaderElection {
	return &LeaderElection{}
}

func (le *LeaderElection) handleElection(action ifs.Action, vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	switch action {
	case ifs.ElectionRequest:
		return le.handleElectionRequest(vnic, msg)
	case ifs.ElectionResponse:
		return le.handleElectionResponse(vnic, msg)
	case ifs.LeaderAnnouncement:
		return le.handleLeaderAnnouncement(vnic, msg)
	case ifs.LeaderHeartbeat:
		return le.handleLeaderHeartbeat(vnic, msg)
	case ifs.LeaderQuery:
		return le.handleLeaderQuery(vnic, msg)
	case ifs.LeaderResign:
		return le.handleLeaderResign(vnic, msg)
	case ifs.LeaderChallenge:
		return le.handleLeaderChallenge(vnic, msg)
	}
	return nil
}

func (le *LeaderElection) handleElectionRequest(vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	localUuid := vnic.Resources().SysConfig().LocalUuid
	senderUuid := msg.Source()

	vnic.Resources().Logger().Debug("Election request from", senderUuid, "for", msg.ServiceName(), "area", msg.ServiceArea(), "local:", localUuid)

	// If sender has lower priority (higher UUID), respond that we're still alive
	if senderUuid < localUuid {
		vnic.Resources().Logger().Debug("Responding to election request and starting own election")
		vnic.Unicast(senderUuid, msg.ServiceName(), msg.ServiceArea(), ifs.ElectionResponse, nil)
		// Start our own election since we have higher priority
		go le.startElection(msg.ServiceName(), msg.ServiceArea(), vnic)
	}

	return nil
}

func (le *LeaderElection) handleElectionResponse(vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	key := makeServiceKey(msg.ServiceName(), msg.ServiceArea())
	info := le.getLeaderInfo(key)
	if info == nil {
		vnic.Resources().Logger().Debug("Election response received but no leader info for", msg.ServiceName(), "area", msg.ServiceArea())
		return nil
	}

	vnic.Resources().Logger().Debug("Election response from", msg.Source(), "for", msg.ServiceName(), "area", msg.ServiceArea())

	info.mtx.Lock()

	// If we're in an election and received a response, a higher-priority node exists
	if info.state == electing {
		vnic.Resources().Logger().Debug("Higher priority node exists, aborting election")
		info.state = hasLeader
		// Don't call timer.Stop() - just change state and the election goroutine will handle it
	}
	info.mtx.Unlock()

	return nil
}

func (le *LeaderElection) handleLeaderAnnouncement(vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	key := makeServiceKey(msg.ServiceName(), msg.ServiceArea())
	info := le.getOrCreateLeaderInfo(key)

	localUuid := vnic.Resources().SysConfig().LocalUuid
	senderUuid := msg.Source()

	vnic.Resources().Logger().Debug("Leader announcement from", senderUuid, "for", msg.ServiceName(), "area", msg.ServiceArea())

	info.mtx.Lock()
	// Accept the leader announcement
	info.leaderUuid = senderUuid
	info.lastHeartbeat = time.Now()

	if senderUuid == localUuid {
		vnic.Resources().Logger().Debug("I am the leader")
		info.state = isLeader
		info.mtx.Unlock()
		le.startHeartbeat(key, msg.ServiceName(), msg.ServiceArea(), vnic)
	} else {
		vnic.Resources().Logger().Debug("Following leader:", senderUuid)
		info.state = hasLeader
		info.mtx.Unlock()
		le.startHeartbeatMonitor(key, msg.ServiceName(), msg.ServiceArea(), vnic)
	}

	vnic.Resources().Logger().Debug("Leader for", msg.ServiceName(), "area", msg.ServiceArea(), "is", senderUuid)
	return nil
}

func (le *LeaderElection) handleLeaderHeartbeat(vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	key := makeServiceKey(msg.ServiceName(), msg.ServiceArea())
	info := le.getLeaderInfo(key)
	if info == nil {
		return nil
	}

	info.mtx.Lock()
	info.leaderUuid = msg.Source()
	info.lastHeartbeat = time.Now()
	info.mtx.Unlock()

	return nil
}

func (le *LeaderElection) handleLeaderQuery(vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	key := makeServiceKey(msg.ServiceName(), msg.ServiceArea())
	info := le.getLeaderInfo(key)

	localUuid := vnic.Resources().SysConfig().LocalUuid
	if info != nil {
		info.mtx.RLock()
		leaderUuid := info.leaderUuid
		state := info.state
		info.mtx.RUnlock()

		if state == isLeader && leaderUuid == localUuid {
			// Respond that we are the leader
			vnic.Unicast(msg.Source(), msg.ServiceName(), msg.ServiceArea(), ifs.LeaderAnnouncement, nil)
		}
	}

	return nil
}

func (le *LeaderElection) handleLeaderResign(vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	key := makeServiceKey(msg.ServiceName(), msg.ServiceArea())
	info := le.getLeaderInfo(key)

	if info != nil {
		info.mtx.Lock()
		if info.leaderUuid == msg.Source() {
			info.leaderUuid = ""
			info.state = idle
			le.stopTimers(info)
		}
		info.mtx.Unlock()

		// Start new election
		go le.startElection(msg.ServiceName(), msg.ServiceArea(), vnic)
	}

	return nil
}

func (le *LeaderElection) handleLeaderChallenge(vnic ifs.IVNic, msg *ifs.Message) ifs.IElements {
	key := makeServiceKey(msg.ServiceName(), msg.ServiceArea())
	info := le.getLeaderInfo(key)

	localUuid := vnic.Resources().SysConfig().LocalUuid
	if info != nil {
		info.mtx.RLock()
		state := info.state
		info.mtx.RUnlock()

		if state == isLeader {
			// Respond with heartbeat to prove we're still the leader
			vnic.Multicast(msg.ServiceName(), msg.ServiceArea(), ifs.LeaderHeartbeat, nil)
		}
	} else if vnic.Resources().SysConfig().LocalUuid == localUuid {
		// No leader info, start election
		go le.startElection(msg.ServiceName(), msg.ServiceArea(), vnic)
	}

	return nil
}

func (le *LeaderElection) startElection(serviceName string, serviceArea byte, vnic ifs.IVNic) {
	key := makeServiceKey(serviceName, serviceArea)
	info := le.getOrCreateLeaderInfo(key)

	info.mtx.Lock()
	if info.state == electing || info.state == isLeader {
		vnic.Resources().Logger().Debug("Election already in progress or already leader for", serviceName, "area", serviceArea)
		info.mtx.Unlock()
		return
	}
	info.state = electing
	info.mtx.Unlock()

	vnic.Resources().Logger().Debug("Starting election for", serviceName, "area", serviceArea)

	// Send election request to all nodes
	vnic.Multicast(serviceName, serviceArea, ifs.ElectionRequest, nil)

	vnic.Resources().Logger().Debug("Waiting", electionTimeout, "for election responses")

	// Wait for responses
	timer := time.NewTimer(electionTimeout)
	info.mtx.Lock()
	info.electionTimer = timer
	info.mtx.Unlock()

	<-timer.C

	info.mtx.Lock()
	currentState := info.state
	vnic.Resources().Logger().Debug("Election timeout elapsed, state:", currentState)

	if currentState == electing {
		// No higher-priority node responded, we are the leader
		localUuid := vnic.Resources().SysConfig().LocalUuid
		info.state = isLeader
		info.leaderUuid = localUuid
		info.mtx.Unlock()

		vnic.Resources().Logger().Debug("Elected as leader for", serviceName, "area", serviceArea)
		vnic.Multicast(serviceName, serviceArea, ifs.LeaderAnnouncement, nil)
		le.startHeartbeat(key, serviceName, serviceArea, vnic)
	} else {
		vnic.Resources().Logger().Debug("Not becoming leader, state changed to:", currentState)
		info.mtx.Unlock()
	}
}

func (le *LeaderElection) startHeartbeat(key string, serviceName string, serviceArea byte, vnic ifs.IVNic) {
	info := le.getLeaderInfo(key)
	if info == nil {
		return
	}

	info.mtx.Lock()
	le.stopTimers(info)
	info.stopChan = make(chan struct{})
	stopChan := info.stopChan
	info.mtx.Unlock()

	go func() {
		ticker := time.NewTicker(heartbeatPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				info.mtx.RLock()
				if info.state != isLeader {
					info.mtx.RUnlock()
					return
				}
				info.mtx.RUnlock()

				vnic.Multicast(serviceName, serviceArea, ifs.LeaderHeartbeat, nil)
			case <-stopChan:
				return
			}
		}
	}()
}

func (le *LeaderElection) startHeartbeatMonitor(key string, serviceName string, serviceArea byte, vnic ifs.IVNic) {
	info := le.getLeaderInfo(key)
	if info == nil {
		return
	}

	info.mtx.Lock()
	le.stopTimers(info)
	info.stopChan = make(chan struct{})
	stopChan := info.stopChan
	info.mtx.Unlock()

	go func() {
		ticker := time.NewTicker(heartbeatTimeout)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				info.mtx.RLock()
				lastHb := info.lastHeartbeat
				state := info.state
				info.mtx.RUnlock()

				if state != hasLeader {
					return
				}

				if time.Since(lastHb) > heartbeatTimeout {
					vnic.Resources().Logger().Debug("Leader heartbeat timeout for", serviceName, "area", serviceArea)
					info.mtx.Lock()
					info.state = idle
					info.leaderUuid = ""
					info.mtx.Unlock()
					le.startElection(serviceName, serviceArea, vnic)
					return
				}
			case <-stopChan:
				return
			}
		}
	}()
}

func (le *LeaderElection) stopTimers(info *leaderInfo) {
	if info.electionTimer != nil {
		info.electionTimer.Stop()
		info.electionTimer = nil
	}
	if info.heartbeatTicker != nil {
		info.heartbeatTicker.Stop()
		info.heartbeatTicker = nil
	}
	if info.stopChan != nil {
		close(info.stopChan)
		info.stopChan = nil
	}
}

func (le *LeaderElection) getLeaderInfo(key string) *leaderInfo {
	info, ok := le.leaders.Load(key)
	if !ok {
		return nil
	}
	return info.(*leaderInfo)
}

func (le *LeaderElection) getOrCreateLeaderInfo(key string) *leaderInfo {
	info, ok := le.leaders.Load(key)
	if ok {
		return info.(*leaderInfo)
	}

	newInfo := &leaderInfo{
		state:         idle,
		lastHeartbeat: time.Now(),
	}
	le.leaders.Store(key, newInfo)
	return newInfo
}

func (le *LeaderElection) StartElectionForService(serviceName string, serviceArea byte, vnic ifs.IVNic) {
	go le.startElection(serviceName, serviceArea, vnic)
}

func (le *LeaderElection) GetLeader(serviceName string, serviceArea byte) string {
	key := makeServiceKey(serviceName, serviceArea)
	info := le.getLeaderInfo(key)
	if info == nil {
		return ""
	}

	info.mtx.RLock()
	defer info.mtx.RUnlock()

	if info.leaderUuid == "" {
		return ""
	}

	return info.leaderUuid
}

func (le *LeaderElection) IsLeader(serviceName string, serviceArea byte, uuid string) bool {
	key := makeServiceKey(serviceName, serviceArea)
	info := le.getLeaderInfo(key)
	if info == nil {
		return false
	}

	info.mtx.RLock()
	defer info.mtx.RUnlock()
	return info.leaderUuid == uuid && info.state == isLeader
}

func makeServiceKey(serviceName string, serviceArea byte) string {
	buff := bytes.Buffer{}
	buff.WriteString(serviceName)
	buff.WriteString("-")
	buff.WriteString(strconv.Itoa(int(serviceArea)))
	return buff.String()
}
