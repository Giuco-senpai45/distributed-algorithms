package main

import (
	"amcds/pb"
	"amcds/pl"
	"amcds/system"
	"amcds/tcp"
	"amcds/utils/log"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/joho/godotenv"
)

func register(pl *pl.PerfectLink, owner string, index int32, hubAddress string) error {
	hubHost, hubPortS, err := net.SplitHostPort(hubAddress)
	if err != nil {
		return err
	}

	hubPort, err := strconv.ParseInt(hubPortS, 10, 32)
	if err != nil {
		return err
	}

	m := &pb.Message{
		Type: pb.Message_PL_SEND,
		PlSend: &pb.PlSend{
			Destination: &pb.ProcessId{
				Host: hubHost,
				Port: int32(hubPort),
			},
			Message: &pb.Message{
				Type: pb.Message_PROC_REGISTRATION,
				ProcRegistration: &pb.ProcRegistration{
					Owner: owner,
					Index: index,
				},
			},
		},
	}

	return pl.Handle(m)
}

type SystemInventory = map[string]*system.System

func main() {
	// parse command line flags
	owner := flag.String("owner", "giuco", "Owner alias")
	hubAddress := flag.String("hub", "127.0.0.1:5000", "Host:Port of the hub")
	port := flag.Int("port", 5004, "Port on which the process runs")
	index := flag.Int("index", 1, "Index of the process")
	flag.Parse()

	host := "127.0.0.1"

	godotenv.Load()

	log.Instantiate()

	networkMessages := make(chan *pb.Message, 4096)

	// link between our system and hub (initial registration)
	// only parse incoming messages (doesn't handle them)
	pl := pl.Create(host, int32(*port), *hubAddress)

	err := register(pl, *owner, int32(*index), *hubAddress)
	if err != nil {
		log.Fatal("Failed to register the process %v", err)
	}

	address := host + ":" + fmt.Sprint(*port)
	l, err := tcp.Listen(address, func(data []byte) {
		m, err := pl.Parse(data)
		if err != nil {
			log.Info("Failed to parse incoming message %v", err)
		}

		log.Debug("Received message hub-mine %v", m)

		networkMessages <- m
	})
	if err != nil {
		log.Fatal("Failed to setup server listener: %v", err)
	}
	defer l.Close()
	log.Info("%v-%v listening on port %v", *owner, *index, *port)

	systems := make(SystemInventory, 0)
	// Process link messages
	go func() {
		for m := range networkMessages {
			switch m.NetworkMessage.Message.Type {
			case pb.Message_PROC_DESTROY_SYSTEM:
				if s, ok := systems[m.SystemId]; ok {
					s.Destroy()
					s = nil
				}
			case pb.Message_PROC_INITIALIZE_SYSTEM:
				s := system.CreateSystem(m.NetworkMessage.Message, host, *owner, *hubAddress, int32(*port), int32(*index))
				s.RegisterAbstractions()
				s.StartEventLoop()
				systems[m.SystemId] = s
			default:
				log.Warn("AM PRIMIT %v", m)
				if s, ok := systems[m.SystemId]; ok {
					s.AddMessage(m)
				} else {
					log.Warn("System %v not initialized", m.SystemId)
				}
			}
		}
	}()

	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, syscall.SIGINT, syscall.SIGTERM)
	<-quitChan
}
