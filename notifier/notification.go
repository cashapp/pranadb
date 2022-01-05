package notifier

import (
	"github.com/golang/protobuf/proto"
	"github.com/squareup/pranadb/errors"

	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
)

type NotificationType int32

const (
	NotificationTypeUnknown NotificationType = iota
	NotificationTypeDDLStatement
	NotificationTypeCloseSession
	NotificationTypeReloadProtobuf
)

func TypeForNotification(notification Notification) NotificationType {
	switch notification.(type) {
	case *notifications.DDLStatementInfo:
		return NotificationTypeDDLStatement
	case *notifications.SessionClosedMessage:
		return NotificationTypeCloseSession
	case *notifications.ReloadProtobuf:
		return NotificationTypeReloadProtobuf
	default:
		return NotificationTypeUnknown
	}
}

// Notification protos live in protos/squareup/cash/pranadb/notifications.proto
type Notification = proto.Message

type NotificationListener interface {
	HandleNotification(notification Notification) error
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
	case NotificationTypeReloadProtobuf:
		msg = &notifications.ReloadProtobuf{}
	default:
		return nil, errors.Errorf("invalid notification type %d", nt)
	}
	return msg, errors.WithStack(b.Unmarshal(msg))
}
