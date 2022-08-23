// This is the clusterid
cluster-id     = "6112451081796031488"
/*
  These are the raft addresses
*/
raft-listen-addresses = [
  "addr1",
  "addr2",
  "addr3"
]

remoting-listen-addresses = [
  "addr4",
  "addr5",
  "addr6"
]

// Numshards
num-shards                        = 50
replication-factor                = 3
data-dir                          = "foo/bar/baz"
test-server                       = false
data-snapshot-entries             = 1001
data-compaction-overhead          = 501
sequence-snapshot-entries         = 2001
sequence-compaction-overhead      = 1001
locks-snapshot-entries            = 101
locks-compaction-overhead         = 51
grpc-api-server-enabled           = true
grpc-api-server-listen-addresses  = [
  "addr7",
  "addr8",
  "addr9"
]
grpc-api-server-tls-enabled       = true
grpc-api-server-tls-key-path      = "grpc-key-path"
grpc-api-server-tls-cert-path     = "grpc-cert-path"
grpc-api-server-tls-client-certs-path = "grpc-client-certs-path"
grpc-api-server-tls-client-auth = "require-and-verify-client-cert"

http-api-server-enabled           = true
http-api-server-listen-addresses  = [
  "addr7-1",
  "addr8-1",
  "addr9-1"
]
http-api-server-tls-key-path      = "http-key-path"
http-api-server-tls-cert-path     = "http-cert-path"
http-api-server-tls-client-certs-path = "http-client-certs-path"
http-api-server-tls-client-auth = "require-and-verify-client-cert"
log-format                        = "json"
log-level                         = "info"
log-file                          = "-"

kafka-brokers = {
  "testbroker" = {
    "client-type" = 1,
    "properties"  = {
      "fakeKafkaID" = "1"
    }
  },
  "testbroker2" = {
    "client-type" = 2,
    "properties"  = {
      "fakeKafkaID" = "23"
      "otherProp" = "xyz"
    }
  }
}

raft-rtt-ms                       = 100
raft-heartbeat-rtt                = 30
raft-election-rtt                 = 300
disable-fsync                     = true
aggregation-cache-size-rows       = 1234
max-process-batch-size            = 777
max-forward-write-batch-size      = 888

dd-profiler-types                 = "HEAP,CPU"
dd-profiler-service-name          = "my-service"
dd-profiler-environment-name      = "playing"
dd-profiler-port                  = 1324
dd-profiler-version-name          = "2.3"
dd-profiler-host-env-var-name     = "FOO_IP"
