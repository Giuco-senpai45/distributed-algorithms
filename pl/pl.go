package pl

import (
	"amcds/pb"
	"amcds/tcp"
	"amcds/utils"
	"amcds/utils/log"
	"errors"
	"net"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type PerfectLink struct {
	host       string
	port       int32
	hubAddress string
	msgQueue   chan *pb.Message
	systemId   string
	parentId   string
	processes  []*pb.ProcessId
}

func Create(host string, port int32, hubAddress string) *PerfectLink {
	return &PerfectLink{
		host:       host,
		port:       port,
		hubAddress: hubAddress,
	}
}

func (pl *PerfectLink) CreateWithProps(systemId string, msgQueue chan *pb.Message, ps []*pb.ProcessId) *PerfectLink {
	pl.systemId = systemId
	pl.msgQueue = msgQueue
	pl.processes = ps

	return pl
}

func (pl PerfectLink) CreateCopyWithParentId(parentAbstraction string) *PerfectLink {
	newPl := pl
	newPl.parentId = parentAbstraction

	return &newPl
}

func (pl *PerfectLink) Handle(m *pb.Message) error {
	switch m.Type {
	case pb.Message_NETWORK_MESSAGE:
		var sender *pb.ProcessId
		for _, p := range pl.processes {
			if p.Host == m.NetworkMessage.SenderHost && p.Port == m.NetworkMessage.SenderListeningPort {
				sender = p
			}
		}
		msg := &pb.Message{
			Type:              pb.Message_PL_DELIVER,
			SystemId:          m.SystemId,
			FromAbstractionId: m.ToAbstractionId,
			ToAbstractionId:   pl.parentId,
			PlDeliver: &pb.PlDeliver{
				Sender:  sender,
				Message: m.NetworkMessage.Message,
			},
		}
		pl.msgQueue <- msg
	case pb.Message_PL_SEND:
		return pl.Send(m)
	default:
		return errors.New("message not supported")
	}

	return nil
}

func (pl *PerfectLink) Send(m *pb.Message) error {
	log.Debug("PLSEND %v", m)
	msgToSend := &pb.Message{
		Type:              pb.Message_NETWORK_MESSAGE,
		SystemId:          pl.systemId,
		FromAbstractionId: pl.parentId + ".pl",
		ToAbstractionId:   m.ToAbstractionId,
		MessageUuid:       uuid.New().String(),
		NetworkMessage: &pb.NetworkMessage{
			Message:             m.PlSend.Message,
			SenderHost:          pl.host,
			SenderListeningPort: pl.port,
		},
	}

	data, err := proto.Marshal(msgToSend)
	if err != nil {
		return err
	}

	address := pl.hubAddress
	if m.PlSend.Destination != nil {
		address = net.JoinHostPort(m.PlSend.Destination.Host, utils.Int32ToString(m.PlSend.Destination.Port))
	}

	return tcp.Send(address, data)
}

func (pl *PerfectLink) Parse(date []byte) (*pb.Message, error) {
	msg := &pb.Message{}
	err := proto.Unmarshal(date, msg)
	log.Debug("PARSE MSG %v", msg)

	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (pl *PerfectLink) Destroy() {}
