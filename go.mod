module github.com/squareup/pranadb

go 1.16

replace github.com/pingcap/tidb  => ./tidb

require (
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/Microsoft/go-winio v0.5.0 // indirect
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5 // indirect
	github.com/alecthomas/kong v0.2.17
	github.com/alecthomas/kong-hcl/v2 v2.0.0-20210826214724-5e9bf8bff126
	github.com/alecthomas/participle/v2 v2.0.0-alpha6
	github.com/alecthomas/repr v0.0.0-20210611225437-1a2716eca9d6
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/cockroachdb/pebble v0.0.0-20210331181633-27fc006b8bfb
	github.com/confluentinc/confluent-kafka-go v1.7.0
	github.com/containerd/continuity v0.1.0 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/golang/protobuf v1.5.0
	github.com/google/btree v1.0.0
	github.com/gotestyourself/gotestyourself v2.2.0+incompatible // indirect
	github.com/lni/dragonboat/v3 v3.3.4
	github.com/opencontainers/image-spec v1.0.1 // indirect
	github.com/opencontainers/runc v1.0.2 // indirect
	github.com/ory/dockertest v3.3.5+incompatible
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/kvproto v0.0.0-20210722113201-768c114017e8
	github.com/pingcap/parser v0.0.0-20210618053735-57843e8185c4
	// tidb version 5.1.2 https://github.com/pingcap/tidb/releases/tag/v5.1.2
	github.com/pingcap/tidb v1.1.0-beta.0.20210917131642-dc079337ef0c
	github.com/pingcap/tipb v0.0.0-20210628060001-1793e022b962
	github.com/pkg/errors v0.9.1
	github.com/segmentio/kafka-go v0.4.17
	github.com/sergi/go-diff v1.1.0
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.7.0
	github.com/tikv/pd v1.1.0-beta.0.20210323123936-c8fa72502f16 // indirect
	golang.org/x/net v0.0.0-20210323141857-08027d57d8cf // indirect
	golang.org/x/sys v0.0.0-20210817190340-bfb29a6856f2 // indirect
	google.golang.org/grpc v1.29.1
	google.golang.org/protobuf v1.26.0
	gotest.tools v2.2.0+incompatible // indirect
)