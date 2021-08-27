# PranaDB

PranaDB is a streaming database...

## To run it

Open four terminals.

In terminal 1:

```
go run cmd/pranadb/main.go --config cfg/example.conf --node-id 0
```

In terminal 2:

```
go run cmd/pranadb/main.go --config cfg/example.conf --node-id 1
```

In terminal 3:

```
go run cmd/pranadb/main.go --config cfg/example.conf --node-id 2
```

In terminal 4, start the CLI

```
go run cmd/prana/main.go
```