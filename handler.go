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

type Handler interface {
	HandleConnection() error
}

type ConnectionHandler struct {
	conn        net.Conn
	connReadTtl time.Duration
	handlerFunc func()
	readChan    chan chanMsg
	writeChan   chan chanMsg
	numWorkers  int
	chanBufSize int
}

func getBsonBytesLength(lengthSlice []byte) int {
	return int(binary.LittleEndian.Uint32(lengthSlice))
}

func NewConnectionHandler(conn net.Conn, connReadTtl time.Duration, handlerFunc func(), numWorkers, chanBufSize int) *ConnectionHandler {
	return &ConnectionHandler{conn: conn,
		connReadTtl: connReadTtl,
		handlerFunc: handlerFunc,
		readChan:    make(chan chanMsg, chanBufSize),
		writeChan:   make(chan chanMsg, chanBufSize),
		numWorkers:  numWorkers,
		chanBufSize: chanBufSize}
}

func (h *ConnectionHandler) WriteChan(msgBody []byte) {
	h.writeChan <- chanMsg{conn: h.conn, msgBody: msgBody}
}

func (h *ConnectionHandler) ReadChan() []byte {
	msg := <-h.readChan
	return msg.msgBody
}

func (h *ConnectionHandler) readConnection() error {
	for {
		ts := time.Now()
		fullBody := make([]byte, 4)
		_, err := h.conn.Read(fullBody)
		lengthBytes := getBsonBytesLength(fullBody)
		if err != nil {
			return err
		}

		if lengthBytes > 0 {
			for {
				if len(fullBody) == lengthBytes {
					h.readChan <- chanMsg{
						conn:    h.conn,
						msgBody: fullBody}
					break
				}
				if time.Since(ts) > h.connReadTtl {
					return fmt.Errorf("Message read ttl %s exceeded time sinse %s\n", h.connReadTtl, time.Since(ts))
				}

				restBytes := lengthBytes - len(fullBody)
				restBody := make([]byte, restBytes)
				receivedBytes, err := h.conn.Read(restBody)

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

func (h *ConnectionHandler) writeConnection() error {
	for {
		msg := <-h.writeChan
		_, err := msg.conn.Write(msg.msgBody)
		if err != nil {
			return fmt.Errorf("error occured while write responce to tcp conn: %s", err.Error())
		}
	}
}

func (h *ConnectionHandler) HandleConnection() error {
	for i := 0; i < h.numWorkers; i++ {
		go h.handlerFunc()
	}
	go h.writeConnection()
	go h.readConnection()
	return nil
}
