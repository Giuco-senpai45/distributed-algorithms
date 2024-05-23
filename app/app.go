package app

import (
	"amcds/pb"
	"amcds/utils"
	"errors"
)

type App struct {
	MsgQueue   chan *pb.Message
	HubAddress string
	HubPort    int32
}

func (app *App) Handle(m *pb.Message) error {
	var msgToSend *pb.Message

	switch m.Type {
	case pb.Message_PL_DELIVER:
		switch m.PlDeliver.Message.Type {
		case pb.Message_APP_BROADCAST:
			msgToSend = &pb.Message{
				Type:              pb.Message_BEB_BROADCAST,
				FromAbstractionId: "app",
				ToAbstractionId:   "app.beb",
				SystemId:          m.SystemId,
				BebBroadcast: &pb.BebBroadcast{
					Message: &pb.Message{
						Type:              pb.Message_APP_VALUE,
						FromAbstractionId: "app",
						ToAbstractionId:   "app",
						SystemId:          m.SystemId,
						AppValue: &pb.AppValue{
							Value: m.PlDeliver.Message.AppBroadcast.Value,
						},
					},
				},
			}
		case pb.Message_APP_VALUE:
			msgToSend = &pb.Message{
				Type:              pb.Message_PL_SEND,
				FromAbstractionId: "app",
				ToAbstractionId:   "app.pl",
				SystemId:          m.SystemId,
				PlSend: &pb.PlSend{
					Destination: &pb.ProcessId{
						Host:  app.HubAddress,
						Port:  app.HubPort,
						Owner: "hub",
					},
					Message: &pb.Message{
						Type:     pb.Message_APP_VALUE,
						AppValue: m.PlDeliver.Message.AppValue,
					},
				},
			}
		case pb.Message_APP_WRITE:
			msgToSend = &pb.Message{
				Type:              pb.Message_NNAR_WRITE,
				FromAbstractionId: "app",
				ToAbstractionId:   "app.nnar[" + m.PlDeliver.Message.AppWrite.Register + "]",
				SystemId:          m.SystemId,
				NnarWrite: &pb.NnarWrite{
					Value: m.PlDeliver.Message.AppWrite.Value,
				},
			}
		case pb.Message_APP_READ:
			msgToSend = &pb.Message{
				Type:              pb.Message_NNAR_READ,
				FromAbstractionId: "app",
				ToAbstractionId:   "app.nnar[" + m.PlDeliver.Message.AppRead.Register + "]",
				SystemId:          m.SystemId,
				NnarRead:          &pb.NnarRead{},
			}
		case pb.Message_APP_PROPOSE:
			msgToSend = &pb.Message{
				Type:              pb.Message_UC_PROPOSE,
				FromAbstractionId: "app",
				ToAbstractionId:   "app.uc[" + m.PlDeliver.Message.AppPropose.Topic + "]",
				UcPropose: &pb.UcPropose{
					Value: m.PlDeliver.Message.AppPropose.Value,
				},
			}
		default:
			return errors.New("app pl message not supported")
		}
	case pb.Message_BEB_DELIVER:
		msgToSend = &pb.Message{
			Type:              pb.Message_PL_SEND,
			FromAbstractionId: "app",
			ToAbstractionId:   "app.pl",
			SystemId:          m.SystemId,
			PlSend: &pb.PlSend{
				Destination: &pb.ProcessId{
					Host:  app.HubAddress,
					Port:  app.HubPort,
					Owner: "hub",
				},
				Message: &pb.Message{
					Type:     pb.Message_APP_VALUE,
					AppValue: m.BebDeliver.Message.AppValue,
				},
			},
		}
	case pb.Message_NNAR_WRITE_RETURN:
		msgToSend = &pb.Message{
			Type:              pb.Message_PL_SEND,
			FromAbstractionId: "app",
			ToAbstractionId:   "app.pl",
			SystemId:          m.SystemId,
			PlSend: &pb.PlSend{
				Destination: &pb.ProcessId{
					Host:  app.HubAddress,
					Port:  app.HubPort,
					Owner: "hub",
				},
				Message: &pb.Message{
					Type:              pb.Message_APP_WRITE_RETURN,
					FromAbstractionId: m.FromAbstractionId,
					ToAbstractionId:   "hub",
					SystemId:          m.SystemId,
					AppWriteReturn: &pb.AppWriteReturn{
						Register: utils.GetRegisterId(m.FromAbstractionId),
					},
				},
			},
		}
	case pb.Message_NNAR_READ_RETURN:
		msgToSend = &pb.Message{
			Type:              pb.Message_PL_SEND,
			FromAbstractionId: "app",
			ToAbstractionId:   "app.pl",
			SystemId:          m.SystemId,
			PlSend: &pb.PlSend{
				Destination: &pb.ProcessId{
					Host:  app.HubAddress,
					Port:  app.HubPort,
					Owner: "hub",
				},
				Message: &pb.Message{
					Type:              pb.Message_APP_READ_RETURN,
					FromAbstractionId: m.FromAbstractionId,
					ToAbstractionId:   "hub",
					SystemId:          m.SystemId,
					AppReadReturn: &pb.AppReadReturn{
						Register: utils.GetRegisterId(m.FromAbstractionId),
						Value:    m.NnarReadReturn.Value,
					},
				},
			},
		}
	case pb.Message_UC_DECIDE:
		msgToSend = &pb.Message{
			Type:              pb.Message_PL_SEND,
			FromAbstractionId: "app",
			ToAbstractionId:   "app.pl",
			PlSend: &pb.PlSend{
				Message: &pb.Message{
					Type:            pb.Message_APP_DECIDE,
					ToAbstractionId: "app",
					AppDecide: &pb.AppDecide{
						Value: m.UcDecide.Value,
					},
				},
			},
		}
	default:
		return errors.New("app message not supported")
	}

	app.MsgQueue <- msgToSend

	return nil
}

func (app *App) Destroy() {}
