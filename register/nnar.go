package register

import (
	"amcds/pb"
	"amcds/utils/log"
	"errors"
)

type NnAtomicRegister struct {
	MsgQueue chan *pb.Message
	N        int32
	Key      string

	Timestamp  int32
	WriterRank int32
	Value      int32

	Acks     int32
	WriteVal *pb.Value
	ReadId   int32
	ReadList map[string]*pb.NnarInternalValue
	Reading  bool
}

func (nnar *NnAtomicRegister) Handle(m *pb.Message) error {
	log.Info("Register handles %v", m)
	var msgToSend *pb.Message
	aId := nnar.getAbstractionId()

	switch m.Type {
	case pb.Message_BEB_DELIVER:
		switch m.BebDeliver.Message.Type {
		case pb.Message_NNAR_INTERNAL_READ:
			incomingReadId := m.BebDeliver.Message.NnarInternalRead.ReadId
			if nnar.ReadId == 0 {
				nnar.ReadId = incomingReadId
			}

			log.Info("Internal read from %v", m.BebDeliver.Message.NnarInternalRead.ReadId)

			msgToSend = &pb.Message{
				Type:              pb.Message_PL_SEND,
				FromAbstractionId: aId,
				ToAbstractionId:   aId + ".pl",
				SystemId:          m.SystemId,
				PlSend: &pb.PlSend{
					Destination: m.BebDeliver.Sender,
					Message: &pb.Message{
						Type:              pb.Message_NNAR_INTERNAL_VALUE,
						FromAbstractionId: aId,
						ToAbstractionId:   aId,
						SystemId:          m.SystemId,
						NnarInternalValue: nnar.buildInternalValue(incomingReadId),
					},
				},
			}
			log.Debug("Broadcasting INTERNAL READ %v", msgToSend)

		case pb.Message_NNAR_INTERNAL_WRITE:
			log.Info("Internal write from %v", m.BebDeliver.Message.NnarInternalWrite.ReadId)
			// update the value
			writerMsg := m.BebDeliver.Message.NnarInternalWrite

			incomingVal := &pb.NnarInternalValue{
				Timestamp:  writerMsg.Timestamp,
				WriterRank: writerMsg.WriterRank,
			}
			currentVal := &pb.NnarInternalValue{
				Timestamp:  nnar.Timestamp,
				WriterRank: nnar.WriterRank,
			}

			if compare(incomingVal, currentVal) == 1 {
				nnar.Timestamp = writerMsg.Timestamp
				nnar.WriterRank = writerMsg.WriterRank
				nnar.updateValue(writerMsg.Value)
			}

			// acknowledge the new val
			msgToSend = &pb.Message{
				Type:              pb.Message_PL_SEND,
				FromAbstractionId: aId,
				ToAbstractionId:   aId + ".pl",
				SystemId:          m.SystemId,
				PlSend: &pb.PlSend{
					Destination: m.BebDeliver.Sender,
					Message: &pb.Message{
						Type:              pb.Message_NNAR_INTERNAL_ACK,
						FromAbstractionId: aId,
						ToAbstractionId:   aId,
						SystemId:          m.SystemId,
						NnarInternalAck: &pb.NnarInternalAck{
							ReadId: nnar.ReadId,
						},
					},
				},
			}
		default:
			return errors.New("message not supported")
		}
	case pb.Message_NNAR_WRITE:
		nnar.ReadId = nnar.ReadId + 1
		nnar.WriteVal = m.NnarWrite.Value
		nnar.Acks = 0
		nnar.Reading = false
		nnar.ReadList = make(map[string]*pb.NnarInternalValue)
		log.Info("Init write %v with readid %v", nnar.WriteVal, nnar.ReadId)

		// broadcast internal read
		msgToSend = &pb.Message{
			Type:              pb.Message_BEB_BROADCAST,
			FromAbstractionId: aId,
			ToAbstractionId:   aId + ".beb",
			SystemId:          m.SystemId,
			BebBroadcast: &pb.BebBroadcast{
				Message: &pb.Message{
					Type:              pb.Message_NNAR_INTERNAL_READ,
					FromAbstractionId: aId,
					ToAbstractionId:   aId,
					SystemId:          m.SystemId,
					NnarInternalRead: &pb.NnarInternalRead{
						ReadId: nnar.ReadId,
					},
				},
			},
		}

	case pb.Message_NNAR_READ:
		nnar.ReadId = nnar.ReadId + 1
		nnar.Acks = 0
		nnar.ReadList = make(map[string]*pb.NnarInternalValue)
		nnar.Reading = true
		log.Info("Init read with readid %v", nnar.ReadId)

		msgToSend = &pb.Message{
			Type:              pb.Message_BEB_BROADCAST,
			FromAbstractionId: aId,
			ToAbstractionId:   aId + ".beb",
			SystemId:          m.SystemId,
			BebBroadcast: &pb.BebBroadcast{
				Message: &pb.Message{
					Type:              pb.Message_NNAR_INTERNAL_READ,
					FromAbstractionId: aId,
					ToAbstractionId:   aId,
					SystemId:          m.SystemId,
					NnarInternalRead: &pb.NnarInternalRead{
						ReadId: nnar.ReadId,
					},
				},
			},
		}

	case pb.Message_PL_DELIVER:
		switch m.PlDeliver.Message.Type {
		case pb.Message_NNAR_INTERNAL_VALUE:
			msgValue := m.PlDeliver.Message.NnarInternalValue
			incomingReadId := msgValue.ReadId
			log.Info("NNAR Internal value for read %v reading (%v)", msgValue.ReadId, nnar.Reading)

			if incomingReadId == nnar.ReadId {
				senderId := string(m.PlDeliver.Sender.Owner) + string(m.PlDeliver.Sender.Index)
				nnar.ReadList[senderId] = msgValue
				nnar.ReadList[senderId].WriterRank = m.PlDeliver.Sender.Rank

				if int32(len(nnar.ReadList)) > nnar.N/2 {
					h := nnar.highest()
					nnar.ReadList = make(map[string]*pb.NnarInternalValue)

					if !nnar.Reading {
						h.Timestamp += 1
						h.WriterRank = nnar.WriterRank
						h.Value = nnar.WriteVal
					}

					msgToSend = &pb.Message{
						Type:              pb.Message_BEB_BROADCAST,
						FromAbstractionId: aId,
						ToAbstractionId:   aId + ".beb",
						SystemId:          m.SystemId,
						BebBroadcast: &pb.BebBroadcast{
							Message: &pb.Message{
								Type:              pb.Message_NNAR_INTERNAL_WRITE,
								FromAbstractionId: aId,
								ToAbstractionId:   aId,
								SystemId:          m.SystemId,
								NnarInternalWrite: &pb.NnarInternalWrite{
									ReadId:     incomingReadId,
									Timestamp:  h.Timestamp,
									WriterRank: h.WriterRank,
									Value:      h.Value,
								},
							},
						},
					}
				}
			}

		case pb.Message_NNAR_INTERNAL_ACK:
			msgValue := m.PlDeliver.Message.NnarInternalAck
			incomingReadId := msgValue.ReadId
			log.Info("NNAR Internal ack for read %v, reading (%v)", msgValue.ReadId, nnar.Reading)

			if incomingReadId == nnar.ReadId {
				nnar.Acks = nnar.Acks + 1
				if nnar.Acks > nnar.N/2 {
					nnar.Acks = 0
					if nnar.Reading {
						nnar.Reading = false
						msgToSend = &pb.Message{
							Type:              pb.Message_NNAR_READ_RETURN,
							FromAbstractionId: aId,
							ToAbstractionId:   "app",
							SystemId:          m.SystemId,
							NnarReadReturn: &pb.NnarReadReturn{
								Value: nnar.buildInternalValue(incomingReadId).Value,
							},
						}
					} else {
						msgToSend = &pb.Message{
							Type:              pb.Message_NNAR_WRITE_RETURN,
							FromAbstractionId: aId,
							SystemId:          m.SystemId,
							ToAbstractionId:   "app",
							NnarWriteReturn:   &pb.NnarWriteReturn{},
						}
					}
				}
			}
		default:
			return errors.New("message not supported")
		}
	default:
		return errors.New("message not supported")
	}

	// log.Info("NNAR SENDING MSG %v", msgToSend)

	if msgToSend != nil {
		nnar.MsgQueue <- msgToSend
	}

	return nil
}

func (nnar *NnAtomicRegister) highest() *pb.NnarInternalValue {
	var highest *pb.NnarInternalValue

	for _, v := range nnar.ReadList {
		if highest == nil {
			highest = v
			continue
		}

		if compare(v, highest) == 1 {
			highest = v
		}
	}

	return highest
}

func (nnar *NnAtomicRegister) getAbstractionId() string {
	return "app.nnar[" + nnar.Key + "]"
}

func (nnar *NnAtomicRegister) buildInternalValue(incomingReadId int32) *pb.NnarInternalValue {
	defined := false
	if nnar.Value != -1 {
		defined = true
	}

	return &pb.NnarInternalValue{
		ReadId:     incomingReadId,
		Timestamp:  nnar.Timestamp,
		WriterRank: nnar.WriterRank,
		Value: &pb.Value{
			V:       nnar.Value,
			Defined: defined,
		},
	}
}

func compare(v1, v2 *pb.NnarInternalValue) int {
	if v1.Timestamp > v2.Timestamp {
		return 1
	}

	if v1.Timestamp < v2.Timestamp {
		return -1
	}

	if v1.Timestamp == v2.Timestamp {
		if v1.WriterRank > v2.WriterRank {
			return 1
		}

		if v1.WriterRank < v2.WriterRank {
			return -1
		}
	}

	return 0
}

func (nnar *NnAtomicRegister) updateValue(v *pb.Value) {
	if v.Defined {
		nnar.Value = v.V
	} else {
		nnar.Value = -1
	}
}

func (nnar *NnAtomicRegister) Destroy() {}
