// This is the clusterid
cluster-id     = "6112451081796031488"
/*
  These are the raft addresses
*/
raft-addresses = [
  "addr1",
  "addr2",
  "addr3"
]

notif-listen-addresses = [
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
enable-api-server                 = true
api-server-listen-addresses       = [
  "addr7",
  "addr8",
  "addr9"
]
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

dd-profiler-types                 = "HEAP,CPU"
dd-profiler-service-name          = "my-service"
dd-profiler-environment-name      = "playing"
dd-profiler-port                  = 1324
dd-profiler-version-name          = "2.3"
dd-profiler-host-env-var-name     = "FOO_IP"
