package consensus

import (
	"amcds/pb"
	"amcds/utils"
	"errors"
)

type Eld struct {
	id        string
	parentId  string
	msgQueue  chan *pb.Message
	processes []*pb.ProcessId
	alive     utils.ProcessMap
	leader    *pb.ProcessId
}

func CreateEld(parentAbstraction, abstractionId string, mQ chan *pb.Message, processes []*pb.ProcessId) *Eld {
	eld := &Eld{
		id:        abstractionId,
		parentId:  parentAbstraction,
		msgQueue:  mQ,
		processes: processes,
		alive:     make(utils.ProcessMap),
		leader:    nil,
	}

	// Set all processes as alive
	for _, p := range processes {
		eld.alive[utils.GetProcessKey(p)] = p
	}

	return eld
}

func (eld *Eld) Handle(m *pb.Message) error {
	switch m.Type {
	case pb.Message_EPFD_SUSPECT:
		key := utils.GetProcessKey(m.EpfdSuspect.Process)
		if _, isAlive := eld.alive[key]; !isAlive {
			delete(eld.alive, key)
		}
	case pb.Message_EPFD_RESTORE:
		eld.alive[utils.GetProcessKey(m.EpfdRestore.Process)] = m.EpfdRestore.Process
	default:
		return errors.New("eld unknown message type")
	}

	return eld.updateLeader()
}

func (eld *Eld) Destroy() {}

func (eld *Eld) updateLeader() error {
	max := utils.GetMaxRank(eld.alive)

	if max == nil {
		return errors.New("could not find process with max rank when electing leader")
	}

	if eld.leader == nil || utils.GetProcessKey(eld.leader) != utils.GetProcessKey(max) {
		eld.leader = max
		eld.msgQueue <- &pb.Message{
			Type:              pb.Message_ELD_TRUST,
			FromAbstractionId: eld.id,
			ToAbstractionId:   eld.parentId,
			EldTrust: &pb.EldTrust{
				Process: eld.leader,
			},
		}
	}

	return nil
}
