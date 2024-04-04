package tcp

import (
	"amcds/utils/log"
	"bufio"
	"encoding/binary"
	"io"
	"net"
)

func Send(address string, data []byte) error {
	c, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer c.Close()

	// send the size of the data in the first 4 bytes
	b := make([]byte, 4)

	// send data in big endian order (standard for networking)
	binary.BigEndian.PutUint32(b, uint32(len(data)))

	// send the actual data
	_, err = c.Write(append(b, data...))

	return err
}

type Handler func(data []byte)

func Listen(address string, handler Handler) (net.Listener, error) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	go func(handler Handler) {
		for {
			// wait for requests
			c, err := l.Accept()
			if err != nil {
				return
			}
			defer c.Close()

			reader := bufio.NewReader(c)

			// read the size of the incoming data buffer
			bufSize := make([]byte, 4)
			_, err = io.ReadFull(reader, bufSize)
			if err != nil {
				log.Warn("Failed to read size of the message: %v", err)
				return
			}

			// initialize a buffer with the sent size
			dataBuf := make([]byte, binary.BigEndian.Uint32(bufSize))
			_, err = io.ReadFull(reader, dataBuf)
			if err != nil {
				log.Warn("Failed to read content of the message: %v", err)
				return
			}

			// send the data to be handled (not your business)
			handler(dataBuf)
		}
	}(handler)

	return l, nil
}
