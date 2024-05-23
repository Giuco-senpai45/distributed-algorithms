package consensus

import (
	"amcds/pb"
	"amcds/utils"
	"errors"
)

type EpState struct {
	ValTs int32
	Value *pb.Value
}

type Ep struct {
	id        string
	parentId  string
	msgQueue  chan *pb.Message
	processes []*pb.ProcessId
	aborted   bool
	ets       int32
	state     *EpState
	tmpVal    *pb.Value
	states    map[string]*EpState
	accepted  int
}

func CreateEp(parentAbstraction, abstractionId string, mQ chan *pb.Message, processes []*pb.ProcessId, ets int32, state *EpState) *Ep {
	return &Ep{
		id:        abstractionId,
		parentId:  parentAbstraction,
		msgQueue:  mQ,
		processes: processes,
		aborted:   false,
		ets:       ets,
		state:     state,
		tmpVal:    &pb.Value{},
		states:    make(map[string]*EpState),
		accepted:  0,
	}
}

func (ep *Ep) Handle(m *pb.Message) error {
	if ep.aborted {
		return nil
	}

	switch m.Type {
	case pb.Message_EP_ABORT:
		ep.msgQueue <- &pb.Message{
			Type:              pb.Message_EP_ABORTED,
			FromAbstractionId: ep.id,
			ToAbstractionId:   ep.parentId,
			EpAborted: &pb.EpAborted{
				Ets:            ep.ets,
				ValueTimestamp: ep.state.ValTs,
				Value:          ep.state.Value,
			},
		}
		ep.aborted = true
	case pb.Message_EP_PROPOSE:
		ep.tmpVal = m.EpPropose.Value

		ep.msgQueue <- &pb.Message{
			Type:              pb.Message_BEB_BROADCAST,
			FromAbstractionId: ep.id,
			ToAbstractionId:   ep.id + ".beb",
			BebBroadcast: &pb.BebBroadcast{
				Message: &pb.Message{
					Type:              pb.Message_EP_INTERNAL_READ,
					FromAbstractionId: ep.id,
					ToAbstractionId:   ep.id,
					EpInternalRead:    &pb.EpInternalRead{},
				},
			},
		}
	case pb.Message_BEB_DELIVER:
		switch m.BebDeliver.Message.Type {
		case pb.Message_EP_INTERNAL_READ:
			ep.msgQueue <- &pb.Message{
				Type:              pb.Message_PL_SEND,
				FromAbstractionId: ep.id,
				ToAbstractionId:   ep.id + ".pl",
				PlSend: &pb.PlSend{
					Destination: m.BebDeliver.Sender,
					Message: &pb.Message{
						Type:              pb.Message_EP_INTERNAL_STATE,
						FromAbstractionId: ep.id,
						ToAbstractionId:   ep.id,
						EpInternalState: &pb.EpInternalState{
							ValueTimestamp: ep.state.ValTs,
							Value:          ep.state.Value,
						},
					},
				},
			}
		case pb.Message_EP_INTERNAL_WRITE:
			ep.state.ValTs = ep.ets
			ep.state.Value = m.BebDeliver.Message.EpInternalWrite.Value
			ep.msgQueue <- &pb.Message{
				Type:              pb.Message_PL_SEND,
				FromAbstractionId: ep.id,
				ToAbstractionId:   ep.id + ".pl",
				PlSend: &pb.PlSend{
					Destination: m.BebDeliver.Sender,
					Message: &pb.Message{
						Type:              pb.Message_EP_INTERNAL_ACCEPT,
						FromAbstractionId: ep.id,
						ToAbstractionId:   ep.id,
						EpInternalAccept:  &pb.EpInternalAccept{},
					},
				},
			}
		case pb.Message_EP_INTERNAL_DECIDED:
			ep.msgQueue <- &pb.Message{
				Type:              pb.Message_EP_DECIDE,
				FromAbstractionId: ep.id,
				ToAbstractionId:   ep.parentId,
				EpDecide: &pb.EpDecide{
					Ets:   ep.ets,
					Value: ep.state.Value,
				},
			}
		default:
			return errors.New("ep beb deliver message not supported")
		}
	case pb.Message_PL_DELIVER:
		switch m.PlDeliver.Message.Type {
		case pb.Message_EP_INTERNAL_STATE:
			// states[q] = (ts,v)
			ep.states[utils.GetProcessKey(m.PlDeliver.Sender)] = &EpState{
				ValTs: m.PlDeliver.Message.EpInternalState.ValueTimestamp,
				Value: m.PlDeliver.Message.EpInternalState.Value,
			}

			// upon #(states) > N/2
			if len(ep.states) > len(ep.processes)/2 {
				hs := ep.highest()
				if hs != nil && hs.Value != nil && hs.Value.Defined {
					ep.tmpVal = hs.Value
				}
				ep.states = make(map[string]*EpState)
				ep.msgQueue <- &pb.Message{
					Type:              pb.Message_BEB_BROADCAST,
					FromAbstractionId: ep.id,
					ToAbstractionId:   ep.id + ".beb",
					BebBroadcast: &pb.BebBroadcast{
						Message: &pb.Message{
							Type:              pb.Message_EP_INTERNAL_WRITE,
							FromAbstractionId: ep.id,
							ToAbstractionId:   ep.id,
							EpInternalWrite: &pb.EpInternalWrite{
								Value: ep.tmpVal,
							},
						},
					},
				}
			}
		case pb.Message_EP_INTERNAL_ACCEPT:
			ep.accepted = ep.accepted + 1

			// upon accepted > N/2
			if ep.accepted > len(ep.processes)/2 {
				ep.accepted = 0
				ep.msgQueue <- &pb.Message{
					Type:              pb.Message_BEB_BROADCAST,
					FromAbstractionId: ep.id,
					ToAbstractionId:   ep.id + ".beb",
					BebBroadcast: &pb.BebBroadcast{
						Message: &pb.Message{
							Type:              pb.Message_EP_INTERNAL_DECIDED,
							FromAbstractionId: ep.id,
							ToAbstractionId:   ep.id,
							EpInternalDecided: &pb.EpInternalDecided{
								Value: ep.tmpVal,
							},
						},
					},
				}
			}
		default:
			return errors.New("ep pl deliver message not supported")
		}
	default:
		return errors.New("ep message not supported")
	}
	return nil
}
func (ep *Ep) Destroy() {}

func (ep *Ep) highest() *EpState {
	state := &EpState{}
	for _, v := range ep.states {
		if v.ValTs > state.ValTs {
			state = v
		}
	}
	return state
}
