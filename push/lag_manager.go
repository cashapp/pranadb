package push

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
	"github.com/squareup/pranadb/push/util"
	"github.com/squareup/pranadb/remoting"
	"strings"
	"sync"
	"time"
)

type LagManager struct {
	shardLags       sync.Map
	lagsTimer       *time.Timer
	broadcastClient remoting.Client
	lock            sync.Mutex
	started         bool
	engine          *Engine
}

var _ util.LagProvider = &LagManager{}

func NewLagManager(engine *Engine, notifAddresses ...string) *LagManager {
	broadcastClient := remoting.NewClient(false, notifAddresses...)
	return &LagManager{broadcastClient: broadcastClient, engine: engine}
}

func (lm *LagManager) GetLag(shardID uint64) time.Duration {
	l, ok := lm.shardLags.Load(shardID)
	if !ok {
		return time.Duration(0)
	}
	lag, ok := l.(time.Duration)
	if !ok {
		panic("not a time.Duration")
	}
	return lag
}

func (lm *LagManager) WaitForAcceptableLag(timeout time.Duration) bool {
	panic("implement me")
}

func (lm *LagManager) Start() error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if lm.started {
		return nil
	}
	lm.engine.cluster.AddHealthcheckListener(lm.broadcastClient.AvailabilityListener())
	if err := lm.broadcastClient.Start(); err != nil {
		return err
	}
	lm.started = true
	lm.broadcastLagsNoLock()
	return nil
}

func (lm *LagManager) Stop() error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if !lm.started {
		return nil
	}
	if lm.lagsTimer != nil {
		lm.lagsTimer.Stop()
	}
	if err := lm.broadcastClient.Stop(); err != nil {
		return err
	}
	lm.started = false
	return nil
}

func (lm *LagManager) HandleMessage(notification remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	lags, ok := notification.(*notifications.LagsMessage)
	if !ok {
		panic("not a LagsMessage")
	}
	for _, lagEntry := range lags.Lags {
		shardID := uint64(lagEntry.ShardId)
		lag := time.Duration(lagEntry.Lag)
		lm.shardLags.Store(shardID, lag)
	}
	return nil, nil
}

func (lm *LagManager) broadcastLags() {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if !lm.started {
		return
	}
	lm.broadcastLagsNoLock()
}

func (lm *LagManager) broadcastLagsNoLock() {
	lm.lagsTimer = time.AfterFunc(1*time.Second, func() {
		msg := lm.engine.getLagsMessage()
		sb := strings.Builder{}
		sb.WriteString("lags are: ")
		for _, entry := range msg.Lags {
			sb.WriteString(fmt.Sprintf("shard_id:%d lag:%d ms ", entry.ShardId, time.Duration(entry.Lag).Milliseconds()))
		}
		log.Debug(sb.String())
		if err := lm.broadcastClient.BroadcastOneway(msg); err != nil {
			log.Errorf("failed to broadcast lags %+v", err)
		} else {
			lm.broadcastLags()
		}
	})
}
