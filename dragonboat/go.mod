module github.com/lni/dragonboat/v3

require (
	github.com/VictoriaMetrics/metrics v1.6.2
	github.com/cockroachdb/pebble v0.0.0-20220726144858-a78491c0086f // Corresponds to branch crl-release-22.1
	github.com/golang/protobuf v1.4.3
	github.com/golang/snappy v0.0.3
	github.com/hashicorp/memberlist v0.2.2
	github.com/juju/ratelimit v1.0.2-0.20191002062651-f60b32039441
	github.com/lni/goutils v1.3.0
	github.com/stretchr/testify v1.7.0
	golang.org/x/sys v0.0.0-20220111092808-5a964db01320
)

go 1.14
