package tcpConHandler

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

type chanMsg struct {
	conn    net.Conn
	msgBody []byte
}
type ConnectionHandler struct {
	readChan    chan chanMsg
	writeChan   chan chanMsg
	numWorkers  int
	chanBufSize int
	connReadTtl time.Duration
}

func getBsonBytesLength(lengthSlice []byte) int {
	return int(binary.LittleEndian.Uint32(lengthSlice))
}

func NewConnectionHandler(numWorkers, chanBufSize int, connReadTtl time.Duration) *ConnectionHandler {
	return &ConnectionHandler{
		readChan:    make(chan chanMsg, chanBufSize),
		writeChan:   make(chan chanMsg, chanBufSize),
		numWorkers:  numWorkers,
		chanBufSize: chanBufSize,
		connReadTtl: connReadTtl}
}

func (h *ConnectionHandler) WriteChan(conn net.Conn, msgBody []byte) {
	h.writeChan <- chanMsg{conn: conn, msgBody: msgBody}
}

func (h *ConnectionHandler) ReadChan() []byte {
	msg := <-h.readChan
	return msg.msgBody
}

func (h *ConnectionHandler) readConnection(conn net.Conn) error {
	for {
		ts := time.Now()
		fullBody := make([]byte, 4)
		_, err := conn.Read(fullBody)
		lengthBytes := getBsonBytesLength(fullBody)
		if err != nil {
			return err
		}

		if lengthBytes > 0 {
			for {
				if len(fullBody) == lengthBytes {
					h.readChan <- chanMsg{
						conn:    conn,
						msgBody: fullBody}
					break
				}
				if time.Since(ts) > h.connReadTtl {
					return fmt.Errorf("Message read ttl %s exceeded time sinse %s\n", h.connReadTtl, time.Since(ts))
				}

				restBytes := lengthBytes - len(fullBody)
				restBody := make([]byte, restBytes)
				receivedBytes, err := conn.Read(restBody)

				if err != nil {
					return err
				}

				if receivedBytes > 0 {
					fullBody = append(fullBody, restBody[:receivedBytes]...)
				}
			}
		}
	}
}

func (h *ConnectionHandler) writeConnection(conn net.Conn) error {
	for {
		msg := <-h.writeChan
		_, err := msg.conn.Write(msg.msgBody)
		if err != nil {
			return fmt.Errorf("error occured while write responce to tcp conn: %s", err.Error())
		}
	}
}

func (h *ConnectionHandler) HandleConnection(conn net.Conn, handlerFunc func()) {
	for i := 0; i < h.numWorkers; i++ {
		go handlerFunc()
	}
	go h.writeConnection(conn)
	go h.readConnection(conn)
}
