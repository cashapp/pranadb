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
remoting-heartbeat-interval       = "76s"
remoting-heartbeat-timeout        = "5s"
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

global-ingest-limit-rows-per-sec = 5000
raft-rtt-ms                       = 100
raft-heartbeat-rtt                = 30
raft-election-rtt                 = 300
