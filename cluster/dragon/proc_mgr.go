package dragon

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lni/dragonboat/v3/raftio"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/interruptor"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/clustermsgs"
	"github.com/squareup/pranadb/remoting"
)

var numLeadersVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "pranadb_number_of_raft_leaders",
	Help: "counter for number of raft leaders on the node, segmented by node_id",
}, []string{"node_id"})

func newProcManager(d *Dragon, serverAddresses []string) *procManager {
	return &procManager{
		nodeID:          uint64(d.cnf.NodeID),
		dragon:          d,
		serverAddresses: serverAddresses,
		numLeadersGuage: numLeadersVec.WithLabelValues(fmt.Sprintf("node-%04d", d.cnf.NodeID)),
	}
}

type procManager struct {
	started          bool
	nodeID           uint64
	dragon           *Dragon
	setLeaderChannel chan raftio.LeaderInfo
	closeWG          sync.WaitGroup
	lock             sync.Mutex
	broadcastTimer   *time.Timer
	broadcastClient  *remoting.Client
	serverAddresses  []string
	leaders          sync.Map
	fillInterruptor  *interruptor.Interruptor
	leadersCount     int64
	numLeadersGuage  prometheus.Gauge
}

// returns leaders for all *data* shards
func (p *procManager) getLeadersMap() map[uint64]uint64 {
	leadersMap := make(map[uint64]uint64)
	p.leaders.Range(func(key, value interface{}) bool {
		shardID := key.(uint64)  //nolint:forcetypeassert
		nodeID := value.(uint64) //nolint:forcetypeassert
		if shardID >= cluster.DataShardIDBase {
			leadersMap[shardID] = nodeID
		}
		return true
	})
	return leadersMap
}

// When an MV or index fill is in progress we can't currently tolerate any leadership changes as this causes change
// of processor and fill must always be performed on the processor for the shard for consistency
// We therefore record the leader shard map on the originating node when the create mv or index is executed and broadcast
// this in the command to all nodes. On receipt on all nodes we check the local leader map is the same and fail if not.
// We also register an interruptor which will cancel the DDL operation if leadership changes before it is complete
// This way we can ensure processors don't change while the operation is in progress
func (p *procManager) registerStartFill(expectedLeaders map[uint64]uint64, interruptor *interruptor.Interruptor) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.fillInterruptor != nil {
		return errors.NewPranaErrorf(errors.DdlCancelled, "fill already in progress")
	}
	leadersMap := p.getLeadersMap()
	same := true
	if len(expectedLeaders) == len(leadersMap) {
		for shardID, nodeID := range leadersMap {
			oNodeID, ok := expectedLeaders[shardID]
			if !ok || oNodeID != nodeID {
				same = false
				break
			}
		}
	} else {
		same = false
	}
	if !same {
		return errors.NewPranaErrorf(errors.DdlRetry, "fill cancelled as leadership changed")
	}
	p.fillInterruptor = interruptor
	return nil
}

func (p *procManager) registerEndFill() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.fillInterruptor = nil
}

func (p *procManager) getLeaderNode(shardID uint64) (uint64, bool) {
	l, ok := p.leaders.Load(shardID)
	if !ok {
		return 0, false
	}
	leader := l.(uint64) //nolint:forcetypeassert
	if !ok {
		panic("not a uint64")
	}
	return leader, true
}

func (p *procManager) LeaderUpdated(info raftio.LeaderInfo) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if !p.started {
		return
	}

	if p.fillInterruptor != nil {
		// A fill is in progress - we can't have leader updated during a fill so we need to interrupt it
		p.fillInterruptor.Interrupt()
		p.fillInterruptor = nil
	}

	log.Debugf("node %d received leader updated cluster id %d node id %d term %d leader id %d",
		p.nodeID, info.ClusterID, info.NodeID, info.Term, info.LeaderID)
	if info.NodeID != p.nodeID+1 {
		panic("received leader info on wrong node")
	}
	// When a later node joins, it won't receive info for membership changes before it joined - but it will
	// know if it becomes the leader for a node, or some other node takes over leadership

	newLeaderNodeID := info.LeaderID - 1 // Dragon node ids start at 1, our node ids start at zero
	if newLeaderNodeID == p.nodeID && info.ClusterID >= cluster.DataShardIDBase {
		// We've become leader for a data shard
		p.setLeaderChannel <- info
	}
	if info.LeaderID > 0 {
		if newLeaderNodeID == p.nodeID {
			prev, ok := p.getLeaderNode(info.ClusterID)
			if !ok || prev != p.nodeID {
				p.addNumberLeadersCount(1)
			}
		}
		p.leaders.Store(info.ClusterID, newLeaderNodeID)
	} else {
		// 0 represents no leader
		pr, ok := p.leaders.LoadAndDelete(info.ClusterID)
		if ok {
			prev := pr.(uint64) //nolint:forcetypeassert
			if prev == p.nodeID {
				p.addNumberLeadersCount(-1)
			}
		}
	}
}

func (p *procManager) handleLeaderInfosMessage(msg *clustermsgs.LeaderInfosMessage) {
	for _, leaderInfo := range msg.LeaderInfos {
		p.leaders.Store(uint64(leaderInfo.ShardId), uint64(leaderInfo.NodeId))
	}
}

func (p *procManager) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	broadcastClient, err := remoting.NewClient(p.dragon.cnf.IntraClusterTLSConfig)
	if err != nil {
		return errors.WithStack(err)
	}
	p.broadcastClient = broadcastClient
	p.setLeaderChannel = make(chan raftio.LeaderInfo, p.dragon.cnf.NumShards)
	p.closeWG.Add(1)
	go p.setLeaderLoop()
	p.started = true
	p.scheduleBroadcast()
	return nil
}

func (p *procManager) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.started = false
	p.broadcastTimer.Stop()
	p.broadcastClient.Stop()
	close(p.setLeaderChannel)
	p.leaders.Range(func(key, _ interface{}) bool {
		p.leaders.Delete(key)
		return true
	})
	p.closeWG.Wait()
}

func (p *procManager) setLeaderLoop() {
	for {
		info, ok := <-p.setLeaderChannel
		if !ok {
			break
		}
		// Now send a propose to the shard saying we're new leader
		// We also pass the raft term in here - this guards against the case where a delayed setLeader for a previous
		// term arrives after the later term. We check the term in the state machine and ignore it if it's not later
		// than the current term
		log.Debugf("node %d setting leader for shard %d term is %d", info.NodeID, info.ClusterID, info.Term)
		if err := p.dragon.setLeader(info.ClusterID, info.Term); err != nil {
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

func (p *procManager) addNumberLeadersCount(delta int64) {
	count := atomic.AddInt64(&p.leadersCount, delta)
	p.numLeadersGuage.Set(float64(count))
}
