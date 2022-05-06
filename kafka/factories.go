package kafka

import (
	"runtime"
)

func NewMessageProviderFactory(topicName string, props map[string]string, groupID string) MessageProviderFactory {
	goarch := runtime.GOARCH
	goos := runtime.GOOS
	if goos == "darwin" && goarch == "arm64" {
		return &SegmentMessageProviderFactory{
			topicName: topicName,
			props:     props,
			groupID:   groupID,
		}
	} else {
		return &ConfluentMessageProviderFactory{
			topicName: topicName,
			props:     props,
			groupID:   groupID,
		}
	}
}
