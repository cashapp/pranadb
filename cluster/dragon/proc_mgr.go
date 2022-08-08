package dragon

import (
	"github.com/lni/dragonboat/v3/raftio"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/clustermsgs"
	"github.com/squareup/pranadb/remoting"
	"sync"
	"time"
)

func newProcManager(d *Dragon, serverAddresses []string) *procManager {
	return &procManager{
		nodeID:           uint64(d.cnf.NodeID),
		dragon:           d,
		setLeaderChannel: make(chan uint64, 10),
		serverAddresses:  serverAddresses,
	}
}

type procManager struct {
	started          bool
	nodeID           uint64
	dragon           *Dragon
	setLeaderChannel chan uint64
	closeWG          sync.WaitGroup
	lock             sync.Mutex
	broadcastTimer   *time.Timer
	broadcastClient  *remoting.Client
	serverAddresses  []string
	leaders          sync.Map
}

func (p *procManager) getLeaderNode(shardID uint64) (uint64, bool) {
	l, ok := p.leaders.Load(shardID)
	if !ok {
		return 0, false
	}
	leader, ok := l.(uint64)
	if !ok {
		panic("not a uint64")
	}
	return leader, true
}

func (p *procManager) setLeaderNode(shardID uint64, nodeID uint64) {
	p.leaders.Store(shardID, nodeID)
}

func (p *procManager) LeaderUpdated(info raftio.LeaderInfo) {
	//log.Infof("node %d received leader updated cluster id %d node id %d term %d leader id %d",
	//   p.nodeID, info.ClusterID, info.NodeID, info.Term, info.LeaderID)
	if info.NodeID != p.nodeID+1 {
		panic("received leader info on wrong node")
	}
	// When a later node joins, it won't receive info for membership changes before it joined - but it will
	// know if it becomes the leader for a node, or some other node takes over leadership

	if info.LeaderID == p.nodeID+1 && info.ClusterID >= cluster.DataShardIDBase {
		// We've become leader for a data shard
		p.setLeaderChannel <- info.ClusterID
	}
	if info.LeaderID > 0 {
		p.setLeaderNode(info.ClusterID, info.LeaderID-1)
	} else {
		// 0 represents no leader
		p.leaders.Delete(info.ClusterID)
	}
}

func (p *procManager) handleLeaderInfosMessage(msg *clustermsgs.LeaderInfosMessage) {
	for _, leaderInfo := range msg.LeaderInfos {
		p.setLeaderNode(uint64(leaderInfo.ShardId), uint64(leaderInfo.NodeId))
	}
}

func (p *procManager) Start() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.broadcastClient = &remoting.Client{}
	p.closeWG.Add(1)
	go p.setLeaderLoop()
	p.started = true
	p.scheduleBroadcast()
}

func (p *procManager) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.started = false
	p.broadcastTimer.Stop()
	p.broadcastClient.Stop()
	close(p.setLeaderChannel)
	p.closeWG.Wait()
}

func (p *procManager) setLeaderLoop() {
	for {
		shardID, ok := <-p.setLeaderChannel
		if !ok {
			break
		}
		// Now send a propose to the shard saying we're new leader
		if err := p.dragon.setLeader(shardID); err != nil {
			log.Errorf("failed to set leader %+v", err)
			break
		}
	}
	p.closeWG.Done()
}

func (p *procManager) scheduleBroadcast() {
	if !p.started {
		return
	}
	p.broadcastTimer = time.AfterFunc(1*time.Second, p.broadcastInfo)
}

func (p *procManager) broadcastInfo() {
	var infos []*clustermsgs.LeaderInfo
	p.leaders.Range(func(key, value interface{}) bool {
		shardID := key.(uint64)  //nolint:forcetypeassert
		nodeID := value.(uint64) //nolint:forcetypeassert
		if nodeID == p.nodeID {
			leaderInfo := &clustermsgs.LeaderInfo{
				ShardId: int64(shardID),
				NodeId:  int64(nodeID),
			}
			infos = append(infos, leaderInfo)
		}
		return true
	})
	if len(infos) > 0 {
		infosMsg := &clustermsgs.LeaderInfosMessage{LeaderInfos: infos}
		p.broadcastClient.BroadcastOneWay(infosMsg, p.serverAddresses...)
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	p.scheduleBroadcast()
}
