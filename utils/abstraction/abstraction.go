package abstraction

import "amcds/pb"

type Abstraction interface {
	Handle(m *pb.Message) error
	Destroy()
}

type Registry = map[string]Abstraction
