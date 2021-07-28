package notifier

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
)

type NotificationType int32

const (
	NotificationTypeUnknown NotificationType = iota
	NotificationTypeDDLStatement
	NotificationTypeCloseSession
)

func TypeForNotification(notification Notification) NotificationType {
	switch notification.(type) {
	case *notifications.DDLStatementInfo:
		return NotificationTypeDDLStatement
	case *notifications.SessionClosedMessage:
		return NotificationTypeCloseSession
	default:
		return NotificationTypeUnknown
	}
}

// Notification protos live in protos/squareup/cash/pranadb/notifications.proto
type Notification = proto.Message

type NotificationListener interface {
	HandleNotification(notification Notification)
}

func SerializeNotification(notification Notification) ([]byte, error) {
	b := proto.NewBuffer(nil)
	nt := TypeForNotification(notification)
	if nt == NotificationTypeUnknown {
		return nil, errors.Errorf("invalid notification type %d", nt)
	}
	err := b.EncodeVarint(uint64(nt))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	err = b.Marshal(notification)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return b.Bytes(), nil
}

func DeserializeNotification(data []byte) (Notification, error) {
	b := proto.NewBuffer(data)
	nt, err := b.DecodeVarint()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var msg Notification
	switch NotificationType(nt) {
	case NotificationTypeDDLStatement:
		msg = &notifications.DDLStatementInfo{}
	case NotificationTypeCloseSession:
		msg = &notifications.SessionClosedMessage{}
	default:
		return nil, errors.Errorf("invalid notification type %d", nt)
	}
	return msg, errors.WithStack(b.Unmarshal(msg))
}
