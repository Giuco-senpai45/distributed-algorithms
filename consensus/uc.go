package consensus

import (
	"amcds/broadcast"
	"amcds/pb"
	"amcds/pl"
	"amcds/utils"
	"amcds/utils/abstraction"
	"errors"
)

type Uc struct {
	id           string
	msgQueue     chan *pb.Message
	abstractions abstraction.Registry
	processes    []*pb.ProcessId
	self         *pb.ProcessId
	pl           *pl.PerfectLink

	val      *pb.Value
	proposed bool
	decided  bool
	ets      int32
	l        *pb.ProcessId
	newTs    int32
	newL     *pb.ProcessId
}

func CreateUc(id string, mQ chan *pb.Message, abstractions abstraction.Registry, processes []*pb.ProcessId, ownProcess *pb.ProcessId, pl *pl.PerfectLink) *Uc {
	uc := &Uc{
		id:           id,
		msgQueue:     mQ,
		abstractions: abstractions,
		processes:    processes,
		self:         ownProcess,
		pl:           pl,

		val:      &pb.Value{},
		proposed: false,
		decided:  false,
		ets:      0,
		l:        utils.GetMaxRankSlice(processes),
		newTs:    0,
		newL:     &pb.ProcessId{},
	}

	uc.addEpAbstractions(&EpState{})

	return uc
}

func (uc *Uc) Handle(m *pb.Message) error {
	switch m.Type {
	case pb.Message_UC_PROPOSE:
		uc.val = m.UcPropose.Value
	case pb.Message_EC_START_EPOCH:
		uc.newTs = m.EcStartEpoch.NewTimestamp
		uc.newL = m.EcStartEpoch.NewLeader

		uc.msgQueue <- &pb.Message{
			Type:              pb.Message_EP_ABORT,
			FromAbstractionId: uc.id,
			ToAbstractionId:   uc.id + uc.getEpId(),
			EpAbort:           &pb.EpAbort{},
		}
	case pb.Message_EP_ABORTED:
		if uc.ets == m.EpAborted.Ets {
			uc.ets = uc.newTs
			uc.l = uc.newL
			uc.proposed = false
			uc.addEpAbstractions(&EpState{
				ValTs: uc.ets,
				Value: m.EpAborted.Value,
			})
		}
	case pb.Message_EP_DECIDE:
		if uc.ets == m.EpDecide.Ets {
			if !uc.decided {
				uc.decided = true
				uc.msgQueue <- &pb.Message{
					Type:              pb.Message_UC_DECIDE,
					FromAbstractionId: uc.id,
					ToAbstractionId:   "app",
					UcDecide: &pb.UcDecide{
						Value: m.EpDecide.Value,
					},
				}
			}
		}
	default:
		return errors.New("uc message not supported")
	}

	return uc.updateLeader()
}
func (uc *Uc) Destroy() {}

func (uc *Uc) addEpAbstractions(state *EpState) {
	aId := uc.id + uc.getEpId()
	uc.abstractions[aId] = CreateEp(uc.id, aId, uc.msgQueue, uc.processes, uc.ets, state)
	uc.abstractions[aId+".beb"] = broadcast.Create(uc.msgQueue, uc.processes, aId+".beb")
	uc.abstractions[aId+".pl"] = uc.pl.CreateCopyWithParentId(aId)
	uc.abstractions[aId+".beb.pl"] = uc.pl.CreateCopyWithParentId(aId + ".beb")
}

func (uc *Uc) getEpId() string {
	return ".ep[" + utils.Int32ToString(uc.ets) + "]"
}

func (uc *Uc) updateLeader() error {
	if utils.GetProcessKey(uc.l) == utils.GetProcessKey(uc.self) && uc.val.Defined && !uc.proposed {
		uc.proposed = true
		uc.msgQueue <- &pb.Message{
			Type:              pb.Message_EP_PROPOSE,
			FromAbstractionId: uc.id,
			ToAbstractionId:   uc.id + uc.getEpId(),
			EpPropose: &pb.EpPropose{
				Value: uc.val,
			},
		}
	}

	return nil
}
