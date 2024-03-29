// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"errors"
	"fmt"
	"sync"

	"github.com/lni/goutils/logutil"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/server"
	"github.com/lni/dragonboat/v3/raftio"
)

var (
	// ErrUnknownTarget is the error returned when the target address of the node
	// is unknown.
	ErrUnknownTarget = errors.New("target address unknown")
)

// INodeRegistry is the local registry interface used to keep all known
// nodes in the system..
type INodeRegistry interface {
	Stop()
	Add(clusterID uint64, nodeID uint64, url string)
	Remove(clusterID uint64, nodeID uint64)
	RemoveCluster(clusterID uint64)
	Resolve(clusterID uint64, nodeID uint64) (string, string, error)
}

var _ INodeRegistry = (*Registry)(nil)
var _ IResolver = (*Registry)(nil)

// Registry is used to manage all known node addresses in the multi raft system.
// The transport layer uses this address registry to locate nodes.
type Registry struct {
	mu          sync.RWMutex
	partitioner server.IPartitioner
	validate    config.TargetValidator
	addr        sync.Map
}

// NewNodeRegistry returns a new Registry object.
func NewNodeRegistry(streamConnections uint64, v config.TargetValidator) *Registry {
	n := &Registry{
		validate: v,
	}
	if streamConnections > 1 {
		n.partitioner = server.NewFixedPartitioner(streamConnections)
	}
	return n
}

// Stop stops the node registry.
func (n *Registry) Stop() {}

func (n *Registry) getTarget(nodeInfo raftio.NodeInfo) (string, bool) {
	t, ok := n.addr.Load(nodeInfo)
	if ok {
		ni, ok := t.(string)
		if !ok {
			panic("not a string")
		}
		return ni, true
	}
	return "", false
}

// Add adds the specified node and its target info to the registry.
func (n *Registry) Add(clusterID uint64, nodeID uint64, target string) {
	if n.validate != nil && !n.validate(target) {
		plog.Panicf("invalid target %s", target)
	}
	key := raftio.GetNodeInfo(clusterID, nodeID)
	v, ok := n.getTarget(key)
	if !ok {
		n.addr.Store(key, target)
	} else {
		if v != target {
			plog.Panicf("inconsistent target for %s, %s:%s",
				logutil.DescribeNode(clusterID, nodeID), v, target)
		}
	}
}

func (n *Registry) getConnectionKey(addr string, clusterID uint64) string {
	if n.partitioner == nil {
		return addr
	}
	return fmt.Sprintf("%s-%d", addr, n.partitioner.GetPartitionID(clusterID))
}

// Remove removes a remote from the node registry.
func (n *Registry) Remove(clusterID uint64, nodeID uint64) {
	key := raftio.GetNodeInfo(clusterID, nodeID)
	n.addr.Delete(key)
}

// RemoveCluster removes all nodes info associated with the specified cluster
func (n *Registry) RemoveCluster(clusterID uint64) {
	n.addr.Range(func(k, value interface{}) bool {
		nodeInfo, ok := k.(raftio.NodeInfo)
		if !ok {
			panic("not a NodeInfo")
		}
		if nodeInfo.ClusterID == clusterID {
			n.addr.Delete(k)
		}
		return true
	})
}

// Resolve looks up the Addr of the specified node.
func (n *Registry) Resolve(clusterID uint64, nodeID uint64) (string, string, error) {
	key := raftio.GetNodeInfo(clusterID, nodeID)
	addr, ok := n.getTarget(key)
	if !ok {
		return "", "", ErrUnknownTarget
	}
	return addr, n.getConnectionKey(addr, clusterID), nil
}
