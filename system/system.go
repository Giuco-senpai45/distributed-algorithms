package system

import (
	"amcds/app"
	"amcds/broadcast"
	"amcds/pb"
	"amcds/pl"
	"amcds/register"
	"amcds/utils"
	"amcds/utils/abstraction"
	"amcds/utils/log"
	"strings"
)

type System struct {
	systemId     string
	msgQueue     chan *pb.Message
	abstractions abstraction.Registry
	hubAddress   string
	ownProcess   *pb.ProcessId
	processes    []*pb.ProcessId
}

func (s *System) StartEventLoop() {
	go s.run()
}

func (s *System) run() {
	for m := range s.msgQueue {
		// check for non-existing registers
		_, ok := s.abstractions[m.ToAbstractionId]

		if !ok {
			if strings.HasPrefix(m.ToAbstractionId, "app.nnar") {
				log.Info("Creating new nnar abstraction for %v", m.ToAbstractionId)
				registerId := utils.GetRegisterId((m.ToAbstractionId))
				log.Info("Register id %v", registerId)
				s.registerNnarAbstractions(registerId)
			}
		}
		handler, ok := s.abstractions[m.ToAbstractionId]

		if !ok {
			log.Debug("Crap aici ca nu stiu sa imi instantitez")
			log.Error("No handler defined for %v", m.ToAbstractionId)
			continue
		}

		log.Debug("["+m.ToAbstractionId+"] handling message %v", m.Type)
		err := handler.Handle(m)
		if err != nil {
			log.Error("Failed to handle message %v", err)
		}
	}
}

func (s *System) RegisterAbstractions() {
	pl := pl.Create(s.ownProcess.Host, s.ownProcess.Port, s.hubAddress).CreateWithProps(s.systemId, s.msgQueue, s.processes)

	s.abstractions["app"] = &app.App{MsgQueue: s.msgQueue}
	s.abstractions["app.pl"] = pl.CreateCopyWithParentId("app")

	s.abstractions["app.beb"] = broadcast.Create(s.msgQueue, s.processes, "app.beb")
	s.abstractions["app.beb.pl"] = pl.CreateCopyWithParentId("app.beb")
}

func (s *System) registerNnarAbstractions(key string) {
	pl := pl.Create(s.ownProcess.Host, s.ownProcess.Port, s.hubAddress).CreateWithProps(s.systemId, s.msgQueue, s.processes)
	aId := "app.nnar[" + key + "]"

	s.abstractions[aId] = &register.NnAtomicRegister{
		MsgQueue:   s.msgQueue,
		N:          int32(len(s.processes)),
		Key:        key,
		Timestamp:  0,
		WriterRank: s.ownProcess.Rank,
		Value:      -1,
		ReadId:     0,
		ReadList:   make(map[string]*pb.NnarInternalValue),
	}
	s.abstractions[aId+".pl"] = pl.CreateCopyWithParentId(aId)
	s.abstractions[aId+".beb"] = broadcast.Create(s.msgQueue, s.processes, aId+".beb")
	s.abstractions[aId+".beb.pl"] = pl.CreateCopyWithParentId(aId + ".beb")
}

func CreateSystem(m *pb.Message, host, owner, hubAddress string, port, index int32) *System {
	log.Debug("Creating system %v", m.SystemId)
	var ownProcess *pb.ProcessId
	for _, p := range m.ProcInitializeSystem.Processes {
		if p.Owner == owner && p.Index == index {
			ownProcess = p
		}
	}

	return &System{
		systemId:     m.SystemId,
		msgQueue:     make(chan *pb.Message, 4096),
		ownProcess:   ownProcess,
		hubAddress:   hubAddress,
		abstractions: make(map[string]abstraction.Abstraction),
		processes:    m.ProcInitializeSystem.Processes,
	}
}

func (s *System) AddMessage(m *pb.Message) {
	log.Debug("Received message for %v with type %v", m.ToAbstractionId, m.Type)
	s.msgQueue <- m
}

func (s *System) Destroy() {
	log.Debug("Destroying system %v", s.systemId)
	for _, a := range s.abstractions {
		a.Destroy()
	}
	close(s.msgQueue)
}
