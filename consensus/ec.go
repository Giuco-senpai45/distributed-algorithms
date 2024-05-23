package consensus

import (
	"amcds/pb"
	"amcds/utils"
	"errors"
)

type Ec struct {
	id        string
	parentId  string
	self      *pb.ProcessId
	msgQueue  chan *pb.Message
	processes []*pb.ProcessId
	trusted   *pb.ProcessId
	lastTs    int32
	ts        int32
}

func CreateEc(parentAbstraction, abstractionId string, ownProcess *pb.ProcessId, mQ chan *pb.Message, processes []*pb.ProcessId) *Ec {
	return &Ec{
		id:        abstractionId,
		parentId:  parentAbstraction,
		self:      ownProcess,
		msgQueue:  mQ,
		processes: processes,
		trusted:   utils.GetMaxRankSlice(processes),
		lastTs:    0,
		ts:        ownProcess.Rank,
	}
}

func (ec *Ec) Handle(m *pb.Message) error {
	switch m.Type {
	case pb.Message_ELD_TRUST:
		ec.trusted = m.EldTrust.Process
		ec.handleSelfTrust()
	case pb.Message_PL_DELIVER:
		switch m.PlDeliver.Message.Type {
		case pb.Message_EC_INTERNAL_NACK:
			ec.handleSelfTrust()
		}
	case pb.Message_BEB_DELIVER:
		switch m.BebDeliver.Message.Type {
		case pb.Message_EC_INTERNAL_NEW_EPOCH:
			newTs := m.BebDeliver.Message.EcInternalNewEpoch.Timestamp
			l := utils.GetProcessKey(m.BebDeliver.Sender)
			trusted := utils.GetProcessKey(ec.trusted)

			if l == trusted && newTs > ec.lastTs {
				ec.lastTs = newTs
				ec.msgQueue <- &pb.Message{
					Type:              pb.Message_EC_START_EPOCH,
					FromAbstractionId: ec.id,
					ToAbstractionId:   ec.parentId,
					EcStartEpoch: &pb.EcStartEpoch{
						NewTimestamp: newTs,
						NewLeader:    m.BebDeliver.Sender,
					},
				}
			} else {
				ec.msgQueue <- &pb.Message{
					Type:              pb.Message_PL_SEND,
					FromAbstractionId: ec.id,
					ToAbstractionId:   ec.id + ".pl",
					PlSend: &pb.PlSend{
						Message: &pb.Message{
							Type:              pb.Message_EC_INTERNAL_NACK,
							FromAbstractionId: ec.id,
							ToAbstractionId:   ec.id,
							EcInternalNack:    &pb.EcInternalNack{},
						},
					},
				}
			}
		default:
			return errors.New("ec unknown beb deliver message type")
		}
	default:
		return errors.New("ec unknown message type")
	}

	return nil
}

func (ec *Ec) Destroy() {}

func (ec *Ec) handleSelfTrust() {
	if utils.GetProcessKey(ec.self) == utils.GetProcessKey(ec.trusted) {
		// increment timestamp with all present processes
		ec.ts = ec.lastTs + int32(len(ec.processes))

		// broadcast new epoch to all processes
		ec.msgQueue <- &pb.Message{
			Type:              pb.Message_BEB_BROADCAST,
			FromAbstractionId: ec.id,
			ToAbstractionId:   ec.id + ".beb",
			BebBroadcast: &pb.BebBroadcast{
				Message: &pb.Message{
					Type:              pb.Message_EC_INTERNAL_NEW_EPOCH,
					FromAbstractionId: ec.id,
					ToAbstractionId:   ec.id,
					EcInternalNewEpoch: &pb.EcInternalNewEpoch{
						Timestamp: ec.ts,
					},
				},
			},
		}
	}
}
