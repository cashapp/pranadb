package loadrunner

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/msggen"
	"time"
)

type PublishCommandFactory struct {
}

func (p *PublishCommandFactory) CreateCommand(loadRunnerNodeID int, commandConfig string) Command {
	return &PublishCommand{
		loadRunnerNodeID: loadRunnerNodeID,
		commandConfig:    commandConfig,
	}
}

func (p *PublishCommandFactory) Name() string {
	return "publish"
}

// PublishCommand is a command which publishes messages Kafka using a message generator
type PublishCommand struct {
	loadRunnerNodeID int
	commandConfig    string
}

type PublishCommandCfg struct {
	// GeneratorName is the name of the generator which actually creates the Kafka messages
	GeneratorName string `json:"generator_name"`
	// TopicName is the name of the topic to send the messages to
	TopicName string `json:"topic_name"`
	// Partitions is the number of partitions in the topic
	Partitions int `json:"partitions"`
	// Delay is the delay between sending each message
	Delay time.Duration `json:"delay"`
	// NumMessages is the total number of messages to send
	NumMessages int64 `json:"num_messages"`
	// IndexStart determines the start ID of the first message sent
	IndexStart int64 `json:"index_start"`
	// KafkaProperties - properties when making the connection to Kafka
	KafkaProperties map[string]string `json:"kafka_properties"`
}

func (p *PublishCommand) Run() error {
	gm, err := msggen.NewGenManager()
	if err != nil {
		return err
	}
	log.Printf("Command config:%s", p.commandConfig)
	cfg := &PublishCommandCfg{}
	if err := json.Unmarshal([]byte(p.commandConfig), cfg); err != nil {
		return err
	}
	err = gm.ProduceMessages(cfg.GeneratorName, cfg.TopicName, cfg.Partitions, cfg.Delay, cfg.NumMessages, cfg.IndexStart, cfg.KafkaProperties)
	log.Printf("publish command completed with err %v", err)
	return err
}
