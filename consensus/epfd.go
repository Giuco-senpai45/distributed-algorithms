package consensus

import (
	"amcds/pb"
	"amcds/utils"
	"amcds/utils/log"
	"errors"
	"time"
)

type EpfdIncreaseTimeout struct {
	id        string
	parentId  string
	msgQueue  chan *pb.Message
	processes []*pb.ProcessId

	alive     utils.ProcessMap
	suspected utils.ProcessMap
	delay     time.Duration
	timer     *time.Timer
}

const delta = 100 * time.Millisecond

func CreateEpfd(parentAbstraction, abstractionId string, mQ chan *pb.Message, processes []*pb.ProcessId) *EpfdIncreaseTimeout {
	epfd := &EpfdIncreaseTimeout{
		id:        abstractionId,
		parentId:  parentAbstraction,
		msgQueue:  mQ,
		processes: processes,

		alive:     make(utils.ProcessMap),
		suspected: make(utils.ProcessMap),
		delay:     delta,
	}

	// Set all processes as alive
	for _, p := range processes {
		epfd.alive[utils.GetProcessKey(p)] = p
	}

	epfd.startTimer(epfd.delay)

	return epfd
}

func (epfd *EpfdIncreaseTimeout) startTimer(delay time.Duration) {
	epfd.timer = time.NewTimer(delay)

	go timerCallBack(epfd)
}

func timerCallBack(epfd *EpfdIncreaseTimeout) {
	<-epfd.timer.C
	msgToSend := &pb.Message{
		Type:              pb.Message_EPFD_TIMEOUT,
		FromAbstractionId: epfd.id,
		ToAbstractionId:   epfd.id,
		EpfdTimeout:       &pb.EpfdTimeout{},
	}

	epfd.msgQueue <- msgToSend
}

func (epfd *EpfdIncreaseTimeout) Handle(m *pb.Message) error {
	switch m.Type {
	case pb.Message_EPFD_TIMEOUT:
		epfd.handleTimeout()

	case pb.Message_PL_DELIVER:
		switch m.PlDeliver.Message.Type {
		case pb.Message_EPFD_INTERNAL_HEARTBEAT_REQUEST:
			epfd.msgQueue <- &pb.Message{
				Type:              pb.Message_PL_SEND,
				FromAbstractionId: epfd.id,
				ToAbstractionId:   epfd.id + ".pl",
				PlSend: &pb.PlSend{
					Message: &pb.Message{
						Type:                       pb.Message_EPFD_INTERNAL_HEARTBEAT_REPLY,
						FromAbstractionId:          epfd.id,
						ToAbstractionId:            epfd.id,
						EpfdInternalHeartbeatReply: &pb.EpfdInternalHeartbeatReply{},
					},
				},
			}
		case pb.Message_EPFD_INTERNAL_HEARTBEAT_REPLY:
			senderProcess := m.PlDeliver.Sender
			key := utils.GetProcessKey(senderProcess)
			epfd.alive[key] = senderProcess
		default:
			return errors.New("epfd pl deliver message type not supported")
		}
	default:
		return errors.New("epfd message type not supported")
	}

	return nil
}

func (epfd *EpfdIncreaseTimeout) handleTimeout() {
	// a dead node came back to life
	for k := range epfd.suspected {
		if _, ok := epfd.alive[k]; ok {
			epfd.delay += delta
			log.Info("Node %v came back to life , increased timeout to %v", k, epfd.delay)
			break
		}
	}

	for _, p := range epfd.processes {
		key := utils.GetProcessKey(p)
		_, isAlive := epfd.alive[key]
		_, isSuspected := epfd.suspected[key]

		if !isAlive && !isSuspected {
			epfd.suspected[key] = p

			// send to ELD
			epfd.msgQueue <- &pb.Message{
				Type:              pb.Message_EPFD_SUSPECT,
				FromAbstractionId: epfd.id,
				ToAbstractionId:   epfd.parentId,
				EpfdSuspect: &pb.EpfdSuspect{
					Process: p,
				},
			}
		} else if isAlive && isSuspected {
			delete(epfd.suspected, key)

			// send to ELD
			epfd.msgQueue <- &pb.Message{
				Type:              pb.Message_EPFD_RESTORE,
				FromAbstractionId: epfd.id,
				ToAbstractionId:   epfd.parentId,
				EpfdRestore: &pb.EpfdRestore{
					Process: p,
				},
			}
		}

		// send heartbeat request
		epfd.msgQueue <- &pb.Message{
			Type:                         pb.Message_EPFD_INTERNAL_HEARTBEAT_REQUEST,
			FromAbstractionId:            epfd.id,
			ToAbstractionId:              epfd.id,
			EpfdInternalHeartbeatRequest: &pb.EpfdInternalHeartbeatRequest{},
		}
	}

	epfd.alive = make(utils.ProcessMap)
	epfd.startTimer(epfd.delay)
}

func (epfd *EpfdIncreaseTimeout) Destroy() {
	epfd.timer.Stop()
}
