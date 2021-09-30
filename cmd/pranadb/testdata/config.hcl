// This is the clusterid
cluster-id = 12345
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
num-shards = 50
replication-factor = 3
data-dir = "foo/bar/baz"
test-server = false
data-snapshot-entries = 1001
data-compaction-overhead = 501
sequence-snapshot-entries = 2001
sequence-compaction-overhead = 1001
locks-snapshot-entries = 101
locks-compaction-overhead = 51
debug = true
notifier-heartbeat-interval = "76s"
enable-api-server = true
api-server-listen-addresses = [
  "addr7",
  "addr8",
  "addr9"
]
api-server-session-timeout = "41s"
api-server-session-check-interval = "6s"
log-format = "json"
log-level = "info"
log-file = "-"

kafka-brokers = {
  "testbroker" = {
    "client-type" = 1,
    "properties" = {
      "fakeKafkaID" = "1"
    }
  }
}
