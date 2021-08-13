package notifier

import "github.com/squareup/pranadb/common"

type NotificationMessage struct {
	requiresResponse bool
	sequence         int64
	notif            Notification
}

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
