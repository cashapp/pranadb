// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/id"
)

func TestNodeHostIDRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nhid := "nhid-12345"
	nhConfig := config.NodeHostConfig{
		RaftAddress: "localhost:27001",
		Gossip: config.GossipConfig{
			BindAddress:      "localhost:26001",
			AdvertiseAddress: "127.0.0.1:26001",
			Seed:             []string{"127.0.0.1:26002"},
		},
	}
	r, err := NewNodeHostIDRegistry(nhid, nhConfig, 1, id.IsNodeHostID)
	if err != nil {
		t.Fatalf("failed to create the registry, %v", err)
	}
	defer r.Stop()
	if r.(*NodeHostIDRegistry).NumMembers() != 1 {
		t.Errorf("num member result unexpected")
	}
	r.Add(123, 456, nhid)
	addr, _, err := r.Resolve(123, 456)
	if err != nil {
		t.Fatalf("failed to get addr, %v", err)
	}
	if addr != nhConfig.RaftAddress {
		t.Errorf("unexpected addr %s", addr)
	}
	// remove node
	r.Remove(123, 456)
	if _, _, err = r.Resolve(123, 456); err != ErrUnknownTarget {
		t.Errorf("removed failed, %v", err)
	}
	// add back
	r.Add(123, 456, nhid)
	addr, _, err = r.Resolve(123, 456)
	if err != nil {
		t.Fatalf("failed to get addr, %v", err)
	}
	if addr != nhConfig.RaftAddress {
		t.Errorf("unexpected addr %s", addr)
	}
	// remove cluster
	r.RemoveCluster(123)
	if _, _, err = r.Resolve(123, 456); err != ErrUnknownTarget {
		t.Fatalf("failed to get addr, %v", err)
	}
}

func TestGossipManagerCanBeCreatedAndStopped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nhid := "nhid-12345"
	nhConfig := config.NodeHostConfig{
		RaftAddress: "localhost:27001",
		Gossip: config.GossipConfig{
			BindAddress:      "localhost:26001",
			AdvertiseAddress: "127.0.0.1:26001",
			Seed:             []string{"127.0.0.1:26002"},
		},
	}
	m, err := newGossipManager(nhid, nhConfig)
	if err != nil {
		t.Fatalf("gossip manager failed to start, %v", err)
	}
	defer m.Stop()
	if m.numMembers() != 1 {
		t.Errorf("unexpected num members")
	}
	if m.advertiseAddress() != "127.0.0.1:26001" {
		t.Errorf("unexpected advertise address, %s", m.advertiseAddress())
	}
	addr, ok := m.GetRaftAddress(nhid)
	if !ok {
		t.Errorf("failed to get raft address")
	}
	if addr != nhConfig.RaftAddress {
		t.Errorf("unexpected raft address, %s", addr)
	}
}

func TestGossipManagerCanGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nhid1 := "nhid-12345"
	nhConfig1 := config.NodeHostConfig{
		RaftAddress: "localhost:27001",
		Expert: config.ExpertConfig{
			TestGossipProbeInterval: 10 * time.Millisecond,
		},
		Gossip: config.GossipConfig{
			BindAddress:      "localhost:26001",
			AdvertiseAddress: "127.0.0.1:26001",
			Seed:             []string{"127.0.0.1:26002"},
		},
	}
	nhid2 := "nhid-67890"
	nhConfig2 := config.NodeHostConfig{
		RaftAddress: "localhost:27002",
		Expert: config.ExpertConfig{
			TestGossipProbeInterval: 10 * time.Millisecond,
		},
		Gossip: config.GossipConfig{
			BindAddress:      "localhost:26002",
			AdvertiseAddress: "127.0.0.1:26002",
			Seed:             []string{"127.0.0.1:26001"},
		},
	}
	m1, err := newGossipManager(nhid1, nhConfig1)
	if err != nil {
		t.Fatalf("gossip manager failed to start, %v", err)
	}
	defer m1.Stop()
	m2, err := newGossipManager(nhid2, nhConfig2)
	if err != nil {
		t.Fatalf("gossip manager failed to start, %v", err)
	}
	defer m2.Stop()
	retry := 0
	for retry < 1000 {
		retry++
		time.Sleep(5 * time.Millisecond)
		if m1.numMembers() != 2 || m2.numMembers() != 2 {
			continue
		}
		addr, ok := m1.GetRaftAddress(nhid2)
		if !ok || addr != nhConfig2.RaftAddress {
			continue
		}
		addr, ok = m2.GetRaftAddress(nhid1)
		if !ok || addr != nhConfig1.RaftAddress {
			continue
		}
		return
	}
	t.Fatalf("failed to complete all queries")
}
