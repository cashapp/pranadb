package remoting

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
	"net"
)

type ClusterMessageType int32

const (
	ClusterMessageTypeUnknown ClusterMessageType = iota + 1
	ClusterMessageDDLStatement
	ClusterMessageCloseSession
	ClusterMessageReloadProtobuf
	ClusterMessageClusterProposeRequest
	ClusterMessageClusterReadRequest
	ClusterMessageClusterProposeResponse
	ClusterMessageClusterReadResponse
)

func TypeForClusterMessage(notification ClusterMessage) ClusterMessageType {
	switch notification.(type) {
	case *notifications.DDLStatementInfo:
		return ClusterMessageDDLStatement
	case *notifications.SessionClosedMessage:
		return ClusterMessageCloseSession
	case *notifications.ReloadProtobuf:
		return ClusterMessageReloadProtobuf
	case *notifications.ClusterProposeRequest:
		return ClusterMessageClusterProposeRequest
	case *notifications.ClusterReadRequest:
		return ClusterMessageClusterReadRequest
	case *notifications.ClusterProposeResponse:
		return ClusterMessageClusterProposeResponse
	case *notifications.ClusterReadResponse:
		return ClusterMessageClusterReadResponse
	default:
		return ClusterMessageTypeUnknown
	}
}

// ClusterMessage protos live in protos/squareup/cash/pranadb/notifications.proto
type ClusterMessage = proto.Message

type ClusterMessageHandler interface {
	HandleMessage(notification ClusterMessage) (ClusterMessage, error)
}

func DeserializeClusterMessage(data []byte) (ClusterMessage, error) {
	if len(data) == 0 {
		return nil, nil
	}
	b := proto.NewBuffer(data)
	nt, err := b.DecodeVarint()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var msg ClusterMessage
	switch ClusterMessageType(nt) {
	case ClusterMessageClusterProposeRequest:
		msg = &notifications.ClusterProposeRequest{}
	case ClusterMessageClusterReadRequest:
		msg = &notifications.ClusterReadRequest{}
	case ClusterMessageClusterProposeResponse:
		msg = &notifications.ClusterProposeResponse{}
	case ClusterMessageClusterReadResponse:
		msg = &notifications.ClusterReadResponse{}
	case ClusterMessageDDLStatement:
		msg = &notifications.DDLStatementInfo{}
	case ClusterMessageCloseSession:
		msg = &notifications.SessionClosedMessage{}
	case ClusterMessageReloadProtobuf:
		msg = &notifications.ReloadProtobuf{}
	default:
		return nil, errors.Errorf("invalid notification type %d", nt)
	}
	return msg, errors.WithStack(b.Unmarshal(msg))
}

type messageType byte

const (
	requestMessageType = iota + 1
	responseMessageType
	heartbeatMessageType
)

type ClusterRequest struct {
	requiresResponse bool
	sequence         int64
	requestMessage   ClusterMessage
}

type ClusterResponse struct {
	sequence        int64
	ok              bool
	errMsg          string
	responseMessage ClusterMessage
}

type messageHandler func(msgType messageType, msg []byte) error

func (n *ClusterRequest) serialize(buff []byte) ([]byte, error) {
	var rrb byte
	if n.requiresResponse {
		rrb = 1
	} else {
		rrb = 0
	}
	buff = append(buff, rrb)
	buff = common.AppendUint64ToBufferLE(buff, uint64(n.sequence))
	nBytes, err := serializeClusterMessage(n.requestMessage)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	buff = append(buff, nBytes...)
	return buff, nil
}

func (n *ClusterRequest) deserialize(buff []byte) error {
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
	n.requestMessage, err = DeserializeClusterMessage(buff[offset:])
	return errors.WithStack(err)
}

func (n *ClusterResponse) serialize(buff []byte) ([]byte, error) {
	var bok byte
	if n.ok {
		bok = 1
	} else {
		bok = 0
	}
	buff = append(buff, bok)
	if !n.ok {
		buff = common.AppendStringToBufferLE(buff, n.errMsg)
	}
	buff = common.AppendUint64ToBufferLE(buff, uint64(n.sequence))
	if n.ok && n.responseMessage != nil {
		nBytes, err := serializeClusterMessage(n.responseMessage)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		buff = append(buff, nBytes...)
	}
	return buff, nil
}

func (n *ClusterResponse) deserialize(buff []byte) error {
	offset := 0
	if bok := buff[offset]; bok == 1 {
		n.ok = true
	} else if bok == 0 {
		n.ok = false
	} else {
		panic(fmt.Sprintf("invalid ok %d", bok))
	}
	offset++
	if !n.ok {
		n.errMsg, offset = common.ReadStringFromBufferLE(buff, offset)
	}
	seq, offset := common.ReadUint64FromBufferLE(buff, offset)
	n.sequence = int64(seq)
	var err error
	n.responseMessage, err = DeserializeClusterMessage(buff[offset:])
	return err
}

func writeMessage(msgType messageType, msg []byte, conn net.Conn) error {
	if msgType == 0 {
		panic("message type written is zero")
	}
	// Heartbeats don't have a body
	if msgType == heartbeatMessageType {
		if _, err := conn.Write([]byte{heartbeatMessageType}); err != nil {
			return err
		}
		return nil
	}
	bytes := make([]byte, 0, messageHeaderSize+len(msg))
	bytes = append(bytes, byte(msgType))
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(len(msg)))
	bytes = append(bytes, msg...)
	_, err := conn.Write(bytes)
	return errors.WithStack(err)
}

func readMessage(handler messageHandler, ch chan error, conn net.Conn, closeAction func()) {
	var msgBuf []byte
	readBuff := make([]byte, readBuffSize)
	msgLen := -1
	for {
		n, err := conn.Read(readBuff)
		if err != nil {
			closeAction()
			// Connection closed
			// We need to close the connection from this side too, to avoid leak of connections in CLOSE_WAIT state
			if err := conn.Close(); err != nil {
				// Ignore
			}
			ch <- nil
			return
		}
		msgBuf = append(msgBuf, readBuff[0:n]...)
		msgType := messageType(msgBuf[0])
		if msgType == heartbeatMessageType {
			// Heartbeats don't have a message body
			if err := handler(msgType, nil); err != nil {
				ch <- err
				return
			}
			msgBuf = msgBuf[1:]
		}
		for len(msgBuf) >= messageHeaderSize {
			if msgLen == -1 {
				u, _ := common.ReadUint32FromBufferLE(msgBuf, 1)
				msgLen = int(u)
			}
			if len(msgBuf) >= messageHeaderSize+msgLen {
				// We got a whole message
				msg := msgBuf[messageHeaderSize : messageHeaderSize+msgLen]
				msg = common.CopyByteSlice(msg)
				if err := handler(msgType, msg); err != nil {
					ch <- err
					return
				}
				// We copy the slice otherwise the backing array won't be gc'd
				msgBuf = common.CopyByteSlice(msgBuf[messageHeaderSize+msgLen:])
				msgLen = -1
			} else {
				break
			}
		}
	}
}
