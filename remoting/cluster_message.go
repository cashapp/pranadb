package remoting

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/clustermsgs"
	"net"
)

type ClusterMessageType int32

const (
	ClusterMessageTypeUnknown ClusterMessageType = iota + 1
	ClusterMessageDDLStatement
	ClusterMessageDDLCancel
	ClusterMessageReloadProtobuf
	ClusterMessageClusterProposeRequest
	ClusterMessageClusterProposeResponse
	ClusterMessageClusterReadRequest
	ClusterMessageClusterReadResponse
	ClusterMessageForwardWriteRequest
	ClusterMessageForwardWriteResponse
	ClusterMessageSourceSetMaxRate
	ClusterMessageLeaderInfos
	ClusterMessageRemotingTestMessage
)

func TypeForClusterMessage(clusterMessage ClusterMessage) ClusterMessageType {
	switch clusterMessage.(type) {
	case *clustermsgs.DDLStatementInfo:
		return ClusterMessageDDLStatement
	case *clustermsgs.DDLCancelMessage:
		return ClusterMessageDDLCancel
	case *clustermsgs.ReloadProtobuf:
		return ClusterMessageReloadProtobuf
	case *clustermsgs.ClusterProposeRequest:
		return ClusterMessageClusterProposeRequest
	case *clustermsgs.ClusterReadRequest:
		return ClusterMessageClusterReadRequest
	case *clustermsgs.ClusterProposeResponse:
		return ClusterMessageClusterProposeResponse
	case *clustermsgs.ClusterReadResponse:
		return ClusterMessageClusterReadResponse
	case *clustermsgs.ClusterForwardWriteRequest:
		return ClusterMessageForwardWriteRequest
	case *clustermsgs.ClusterForwardWriteResponse:
		return ClusterMessageForwardWriteResponse
	case *clustermsgs.SourceSetMaxIngestRate:
		return ClusterMessageSourceSetMaxRate
	case *clustermsgs.LeaderInfosMessage:
		return ClusterMessageLeaderInfos
	case *clustermsgs.RemotingTestMessage:
		return ClusterMessageRemotingTestMessage
	default:
		return ClusterMessageTypeUnknown
	}
}

// ClusterMessage protos live in protos/squareup/cash/pranadb/clustermsgs.proto
type ClusterMessage = proto.Message

type ClusterMessageHandler interface {
	HandleMessage(clusterMessage ClusterMessage) (ClusterMessage, error)
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
		msg = &clustermsgs.ClusterProposeRequest{}
	case ClusterMessageClusterReadRequest:
		msg = &clustermsgs.ClusterReadRequest{}
	case ClusterMessageClusterProposeResponse:
		msg = &clustermsgs.ClusterProposeResponse{}
	case ClusterMessageClusterReadResponse:
		msg = &clustermsgs.ClusterReadResponse{}
	case ClusterMessageForwardWriteRequest:
		msg = &clustermsgs.ClusterForwardWriteRequest{}
	case ClusterMessageForwardWriteResponse:
		msg = &clustermsgs.ClusterForwardWriteResponse{}
	case ClusterMessageDDLStatement:
		msg = &clustermsgs.DDLStatementInfo{}
	case ClusterMessageDDLCancel:
		msg = &clustermsgs.DDLCancelMessage{}
	case ClusterMessageReloadProtobuf:
		msg = &clustermsgs.ReloadProtobuf{}
	case ClusterMessageSourceSetMaxRate:
		msg = &clustermsgs.SourceSetMaxIngestRate{}
	case ClusterMessageLeaderInfos:
		msg = &clustermsgs.LeaderInfosMessage{}
	case ClusterMessageRemotingTestMessage:
		msg = &clustermsgs.RemotingTestMessage{}
	default:
		return nil, errors.Errorf("invalid notification type %d", nt)
	}
	return msg, errors.WithStack(b.Unmarshal(msg))
}

type messageType byte

const (
	requestMessageType = iota + 1
	responseMessageType
)

type ClusterRequest struct {
	requiresResponse bool
	sequence         int64
	requestMessage   ClusterMessage
}

type ClusterResponse struct {
	sequence        int64
	ok              bool
	errCode         int
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
		buff = common.AppendUint32ToBufferLE(buff, uint32(n.errCode))
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
		var code uint32
		code, offset = common.ReadUint32FromBufferLE(buff, offset)
		n.errCode = int(code)
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
	bytes := make([]byte, 0, messageHeaderSize+len(msg))
	bytes = append(bytes, byte(msgType))
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(len(msg)))
	bytes = append(bytes, msg...)
	_, err := conn.Write(bytes)
	return errors.WithStack(err)
}

func readMessage(handler messageHandler, conn net.Conn, closeAction func()) {
	defer common.PanicHandler()
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
			return
		}
		msgBuf = append(msgBuf, readBuff[0:n]...)
		msgType := messageType(msgBuf[0])
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
					log.Errorf("failed to handle message %v", err)
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

func serializeClusterMessage(clusterMessage ClusterMessage) ([]byte, error) {
	b := proto.NewBuffer(nil)
	nt := TypeForClusterMessage(clusterMessage)
	if nt == ClusterMessageTypeUnknown {
		return nil, errors.Errorf("invalid cluster message type %d", nt)
	}
	err := b.EncodeVarint(uint64(nt))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	err = b.Marshal(clusterMessage)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return b.Bytes(), nil
}
