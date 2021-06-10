package raft

import (
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"testing"
)

func TestDragonboat(t *testing.T) {

	nhc := config.NodeHostConfig{}

	_, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		panic(err)
	}

}
