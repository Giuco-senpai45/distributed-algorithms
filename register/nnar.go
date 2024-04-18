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
	ReadList map[int32]*pb.NnarInternalValue
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
			if nnar.ReadId == 0 {
				nnar.ReadId = m.BebDeliver.Message.NnarInternalRead.ReadId
			}
			log.Info("Internal read from %v", m.BebDeliver.Message.NnarInternalRead.ReadId)
			log.Info("BCAST MSJ INTERNAL READ %v", m)
			msgToSend = &pb.Message{
				Type:              pb.Message_PL_SEND,
				FromAbstractionId: aId,
				ToAbstractionId:   aId + ".pl",
				PlSend: &pb.PlSend{
					Destination: m.BebDeliver.Sender,
					Message: &pb.Message{
						Type:              pb.Message_NNAR_INTERNAL_VALUE,
						FromAbstractionId: aId,
						ToAbstractionId:   aId,
						NnarInternalValue: nnar.buildInternalValue(),
					},
				},
			}
			log.Debug("SENDING MSG %v", msgToSend)
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
				PlSend: &pb.PlSend{
					Destination: m.BebDeliver.Sender,
					Message: &pb.Message{
						Type:              pb.Message_NNAR_INTERNAL_ACK,
						FromAbstractionId: aId,
						ToAbstractionId:   aId,
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
		nnar.ReadList = make(map[int32]*pb.NnarInternalValue)
		log.Info("Init write %v with readid %v", nnar.WriteVal, nnar.ReadId)

		// broadcast internal read
		msgToSend = &pb.Message{
			Type:              pb.Message_BEB_BROADCAST,
			FromAbstractionId: aId,
			ToAbstractionId:   aId + ".beb",
			BebBroadcast: &pb.BebBroadcast{
				Message: &pb.Message{
					Type:              pb.Message_NNAR_INTERNAL_READ,
					FromAbstractionId: aId,
					ToAbstractionId:   aId,
					NnarInternalRead: &pb.NnarInternalRead{
						ReadId: nnar.ReadId,
					},
				},
			},
		}
	case pb.Message_NNAR_READ:
		nnar.ReadId = nnar.ReadId + 1
		nnar.Acks = 0
		nnar.ReadList = make(map[int32]*pb.NnarInternalValue)
		nnar.Reading = true
		log.Info("Init read with readid %v", nnar.ReadId)

		msgToSend = &pb.Message{
			Type:              pb.Message_BEB_BROADCAST,
			FromAbstractionId: aId,
			ToAbstractionId:   aId + ".beb",
			BebBroadcast: &pb.BebBroadcast{
				Message: &pb.Message{
					Type:              pb.Message_NNAR_INTERNAL_READ,
					FromAbstractionId: aId,
					ToAbstractionId:   aId,
					NnarInternalRead: &pb.NnarInternalRead{
						ReadId: nnar.ReadId,
					},
				},
			},
		}
	case pb.Message_PL_DELIVER:
		log.Debug("MACAR INTRU ACI")
		switch m.PlDeliver.Message.Type {
		case pb.Message_NNAR_INTERNAL_VALUE:
			msgValue := m.PlDeliver.Message.NnarInternalValue
			log.Info("NNAR Internal value for read %v reading (%v)", msgValue.ReadId, nnar.Reading)
			if msgValue.ReadId == nnar.ReadId {
				nnar.ReadList[m.PlDeliver.Sender.Port] = msgValue
				nnar.ReadList[m.PlDeliver.Sender.Port].WriterRank = m.PlDeliver.Sender.Rank

				if int32(len(nnar.ReadList)) > nnar.N/2 {
					h := nnar.highest()
					nnar.ReadList = make(map[int32]*pb.NnarInternalValue)

					if !nnar.Reading {
						h.Timestamp += 1
						h.WriterRank = nnar.WriterRank
						h.Value = nnar.WriteVal
					}

					msgToSend = &pb.Message{
						Type:              pb.Message_BEB_BROADCAST,
						FromAbstractionId: aId,
						ToAbstractionId:   aId + ".beb",
						BebBroadcast: &pb.BebBroadcast{
							Message: &pb.Message{
								Type:              pb.Message_NNAR_INTERNAL_WRITE,
								FromAbstractionId: aId,
								ToAbstractionId:   aId,
								NnarInternalWrite: &pb.NnarInternalWrite{
									ReadId:     nnar.ReadId,
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

			log.Info("NNAR Internal ack for read %v, reading (%v)", msgValue.ReadId, nnar.Reading)
			if msgValue.ReadId == nnar.ReadId {
				nnar.Acks = nnar.Acks + 1
				if nnar.Acks > nnar.N/2 {
					nnar.Acks = 0
					if nnar.Reading {
						nnar.Reading = false
						msgToSend = &pb.Message{
							Type:              pb.Message_NNAR_READ_RETURN,
							FromAbstractionId: aId,
							ToAbstractionId:   "app",
							NnarReadReturn: &pb.NnarReadReturn{
								Value: nnar.buildInternalValue().Value,
							},
						}
					} else {
						msgToSend = &pb.Message{
							Type:              pb.Message_NNAR_WRITE_RETURN,
							FromAbstractionId: aId,
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

	log.Info("NNAR SENDING MSG %v", msgToSend)

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

func (nnar *NnAtomicRegister) buildInternalValue() *pb.NnarInternalValue {
	defined := false
	if nnar.Value != -1 {
		defined = true
	}

	return &pb.NnarInternalValue{
		ReadId:    nnar.ReadId,
		Timestamp: nnar.Timestamp,
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

	if v1.WriterRank > v2.WriterRank {
		return 1
	}

	if v1.WriterRank < v2.WriterRank {
		return -1
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
