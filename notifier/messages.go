package notifier

import (
	"fmt"
	"github.com/squareup/pranadb/common"
	"net"
)

type messageType byte

const (
	notificationMessageType = iota
	notificationResponseMessageType
	heartbeatMessageType
)

type NotificationMessage struct {
	requiresResponse bool
	sequence         int64
	notif            Notification
}

type NotificationResponse struct {
	sequence int64
	ok       bool
}

type messageHandler func(msgType messageType, msg []byte)

func (n *NotificationMessage) serialize(buff []byte) ([]byte, error) {
	var rrb byte
	if n.requiresResponse {
		rrb = 1
	} else {
		rrb = 0
	}
	buff = append(buff, rrb)
	buff = common.AppendUint64ToBufferLE(buff, uint64(n.sequence))
	nBytes, err := serializeNotification(n.notif)
	if err != nil {
		return nil, err
	}
	buff = append(buff, nBytes...)
	return buff, nil
}

func (n *NotificationMessage) deserialize(buff []byte) error {
	offset := 0
	if rrb := buff[offset]; rrb == 1 {
		n.requiresResponse = true
	} else if rrb == 0 {
		n.requiresResponse = false
	} else {
		panic("invalid requires response byte")
	}
	offset++
	var seq uint64
	seq, offset = common.ReadUint64FromBufferLE(buff, offset)
	n.sequence = int64(seq)
	var err error
	n.notif, err = DeserializeNotification(buff[offset:])
	return err
}

func (n *NotificationResponse) serialize(buff []byte) []byte {
	var bok byte
	if n.ok {
		bok = 1
	} else {
		bok = 0
	}
	buff = append(buff, bok)
	buff = common.AppendUint64ToBufferLE(buff, uint64(n.sequence))
	return buff
}

func (n *NotificationResponse) deserialize(buff []byte) {
	offset := 0
	if bok := buff[offset]; bok == 1 {
		n.ok = true
	} else if bok == 0 {
		n.ok = false
	} else {
		panic(fmt.Sprintf("invalid ok %d", bok))
	}
	offset++
	seq, _ := common.ReadUint64FromBufferLE(buff, offset)
	n.sequence = int64(seq)
}

func writeMessage(msgType messageType, msg []byte, conn net.Conn) error {
	bytes := make([]byte, 0, messageHeaderSize+len(msg))
	bytes = append(bytes, byte(msgType))
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(len(msg)))
	bytes = append(bytes, msg...)
	_, err := conn.Write(bytes)
	return err
}

func readMessage(handler messageHandler, ch chan error, conn net.Conn) {
	var msgBuf []byte
	readBuff := make([]byte, readBuffSize)
	msgLen := -1
	for {
		n, err := conn.Read(readBuff)
		if err != nil {
			// Connection closed
			ch <- nil
			return
		}
		msgBuf = append(msgBuf, readBuff[0:n]...)
		var msgType messageType
		for len(msgBuf) >= messageHeaderSize {
			if msgLen == -1 {
				msgType = messageType(msgBuf[0])
				u, _ := common.ReadUint32FromBufferLE(msgBuf, 1)
				msgLen = int(u)
			}
			if len(msgBuf) >= messageHeaderSize+msgLen {
				// We got a whole message
				msg := msgBuf[messageHeaderSize : messageHeaderSize+msgLen]
				msg = common.CopyByteSlice(msg)
				handler(msgType, msg)
				// We copy the slice otherwise the backing array won't be gc'd
				msgBuf = common.CopyByteSlice(msgBuf[messageHeaderSize+msgLen:])
				msgLen = -1
			} else {
				break
			}
		}
	}
}
