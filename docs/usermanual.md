# PranaDB User Manual

## What is PranaDB?

PranaDB is a *streaming database*.

PranaDB ingests data streams from external sources - e.g. Apache Kafka topics, and allows you to define computations,
usually expressed in standard SQL, over those streams. The results of those computations are stored persistently. We
call these *materialized views* and they update incrementally and continuously as new data arrives.

Materialized views are defined using standard SQL and you access the data in those materialized views by executing
queries, also using standard SQL, just as you would with a traditional relational database.

We also wish to support the execution of custom processing logic - external functions from PranaDB. This essentially
will enable you to create custom materialized views where it is not feasible to define the processing in terms of SQL.
Going ahead, these functions could be defined as gRPC endpoints, AWS lambdas, or in other ways.

PranaDB is a real distributed database, it is not simply an in-memory cache, and is designed from the beginning to scale
horizontally to effectively support views with very large amounts of data. Once ingested, it owns the data, it does not
use Apache Kafka to store intermediate state.

Going ahead we also want to make it possible to publish events *directly* to PranaDB and to be able to consume them as
stream, in much the same as you would with Apache Kafka. PranaDB then becomes a hybrid of an event streaming platform (
such as Kafka) and a relational database (such as MySQL).

Imagine a Kafka where you can query events in your topics using SQL and where you can make persistent projections of
those events. With PranaDB the tricky problems of consistency, scalability and availability are handled for you, and you
simply use SQL to define your views or retrieve data, and call functions written in your programming language of choice
to express custom computations.

That's the vision behind PranaDB and we believe it's a very powerful proposition.

## Example

Here's a quick example.

We have a Kafka topic which contains transaction events for all customers. We create a *source*. A PranaDB source is
like a database table where the data gets filled from a Kafka topic:

```
create source all_transactions(
     transaction_id varchar,
     customer_id bigint,
     transaction_time timestamp,
     amount decimal(10, 2),
     primary key (payment_id)
 ) with (
     brokername = "testbroker",
     topicname = "transactions",
     ...
 );
```

As you can see, this is very similar to a `CREATE TABLE...` statement in SQL.

The source automatically updates as new data arrives from the topic.

You can then query it from your application using standard SQL:

```
select * from all_transactions where customer_id = 12345678;
```

We can now create a materialized view that maintains each customer's current balance.

```
create materialized view customer_balances as
select customer_id, sum(amount) as balance from all_transactions group by customer_id
```

You can now query that from your app to get the customer's current balance

```
select total from customer_balances where customer_id = 12345678
```

## Running the server

_The following assumes you have checked out the GitHub project and you have golang installed. You can then build and run
the server just using `go run ...`. This is a convenient way to play around with the project. Alternatively you could
build the Prana executables first and execute them directly anywhere without needing go to be installed._

PranaDB requires a minimum of three nodes to form a cluster - data is replicated to three nodes by default.

To run the server:

```shell
pranadb --config cfg/example.conf --node-id 0
```

The parameter `node-id` identifies the server in the cluster. If there are three nodes in the values of `node-id` must
be 0, 1 and 2 on different nodes.

The parameter `config` is the path to the server configuration file. PranaDB ships with an example configuration
in `cfg/example.conf`
which is designed for demos and playing around where all nodes are running on your local machine.

## Running the client

PranaDB includes a command line client that you can use for interacting with the Prana cluster and doing things like
executing DDL (creating or dropping sources or materialized views) and executing queries.

First make sure you have a PranaDB cluster running. Then, to run the client:

```shell
go run cmd/prana/main.go shell
```

By default, the client will attempt to connect to a PranaDB instance running on `localhost:6584`, if your PranaDB
instance is running on a different host:port then you can specify this on the command line:

```shell
go run cmd/prana/main.go shell --addr myhost:7654
```

## The PranaDB mental model

The PranaDB mental model is very simple and should be second nature to you if you've had experience with relational
databases. The SQL used in queries is standard and we don't introduce any new or confusing syntax. The language used (
DDL) for creating or dropping PranaDB entities such as sources and materialized views is very similar to the syntax
you'd use when creating or dropping entities in a relational database such as a table.

### Schemas

A schema defines a namespace which can contain entities such as sources and materialized views. A PranaDB cluster can
contain many schemas, and each schema can contain many entities. Entities in one schema cannot see entities in another
schema.

When interacting with PranaDB using the client you need to tell it what schema to use. This is done using the `use`
statement:

```
pranadb> use schema my_app_schema;
0 rows returned
```

You won't be able to access any sources or materialized views unless you are using a schema.

### `sys` schema

There is a special schema called `sys` which contains metadata for PranaDB. E.g. there is a table in `sys`
called `tables`
which contains the meta data for all sources and materialized views. You can execute queries against it like any other
table.

```
pranadb> use sys;
0 rows returned
pranadb> select * from tables;
|id|kind|schema_name|name|table_info|topic_info|query|mv_name|
0 rows returned
```

### Sources

PranaDB ingests data from external feeds such as Kafka topics into entities called _sources_. You can think of a source
as basically like a database table that you'd find in any relational database - it has rows and columns, and the columns
can have different types. Just like a relational database table you can query it by executing a query expressed using
SQL.

The main difference between a relational database table and a PranaDB source is that you can directly `insert`, `update`
or `delete` rows in it from a client. With PranaDB you can't do that. The only way that rows in a PranaDB source get
inserted, updated or deleted is by events being consumed from the Kafka topic and being translated to inserts/updates or
deletes in the source.

Once you've created a source you can execute queries against it from your application, similarly to how you would with
any relational database.

#### Creating a source

You create a source with a `create source` statement. This is quite similar to a `create table` statement that you'd
find with a traditional database. In the `create source` statement you provide the name of the source and the name and
types of the columns.

You also need to provide a primary key for the source. Incoming data with the same value of the primary key *upserts*
data in the source (i.e either inserts or updates any existing data).

Data is laid out in storage in primary key order which makes queries which lookup or scan ranges of the primary key
efficient. You can also create secondary indexes on other columns of the source. This can help avoid scanning the entire
source for queries that lookup values or ranges of columns other than the primary key.

Where a `create source` statement differs from a relational database `create table` statement is you also need to tell
PranaDB *where* to get the data from and *how* to decode the incoming data, and *how* parts of the incoming message map
to columns of the source.

Here's an example `create source` statement

```
create source all_transactions(
     transaction_id varchar,
     customer_id bigint,
     transaction_time timestamp,
     amount decimal(10, 2),
     primary key (transaction_id)
 ) with (
     brokername = "testbroker",
     topicname = "transactions",
     keyencoding = "stringbytes",
     valueencoding = "json",
     columnselectors = (
         meta("key"),
         customer_id,
         meta("timestamp"),
         amount
     )
 ); 
```

This creates a source called `all_transactions` with columns `transaction_id`, `customer_id`, `transaction_time` and
`amount`.

The source data is ingested from a Kafka topic called `transactions`, from a Kafka broker named `testbroker`.
(The config for the broker is defined in the PranaDB server configuration file). The rest of the configuration describes
how the data is encoded in the incoming Kafka message and how to retrieve the column values from the Kafka message.

For a full description of how to configure a source, please consult the source reference.

#### Dropping a source

You drop a source with a `drop source` statement. For example:

```
drop source all_transactions;
```

Dropping a source deletes the source from PranaDB including all the data it contains. Dropping a source is irreversible.

You won't be able to drop a source if it has child materialized views. You'll have to drop any children first.

### Materialized views

There are two *table-like* entities in PranaDB - one is a source and the other is a materialized view. A source maps
data in a feed, such as a Kafka topic into a table structure, whereas a materialized view defines a table structure
based on input from one or more sources or other materialized views. How the data is mapped from the inputs into the
view is defined by a SQL query.

As new data arrives on the inputs of the materialized view it is processed according to the SQL query and the data in
the materialized view is incrementally upserted or deleted.

Internally, the materialized view is stored as a persistent table structure, just like a source, and is distributed
across all the shards in the cluster.

Also, like a source, once you've created your materialized view you can execute queries against it from your app, as you
would with any relational database.

Materialized views can take input from one or more other materialized views or sources, so the set of materialized views
in a schema form a directed acyclic graph (DAG). As data is ingested into PranaDB it flows through the DAG, updating the
materialized views, in near real-time.

#### Creating a materialized view

You create a materialized view with a `create materialized view` statement. In the `create materialized view` you
provide the name of the materialized view and the SQL query that defines it.

Here's an example, which creates a materialized view which shows an account balance per customer based on
the `all_transactions`
source we used earlier:

```
create materialized view customer_balances as
select customer_id, sum(amount) as balance from all_transactions group by customer_id;
```

Materialized views can be chained, so we can create another materialized view which maintains only the large balances

```
create materialized view large_customer_balances as
select customer_id, balance from customer_balances where balance > 1000000;
```

#### Dropping a materialized view

You drop a materialized view with a `drop materialized view` statement.

```
drop materialized view all_transactions;
```

Dropping a materialized view deletes the materialized view from PranaDB including all the data it contains. Dropping a
materialized view is irreversible.

You won't be able to drop a materialized view if it has child materialized views. You'll have to drop any children
first.

#### SQL supported in materialized views

We support a sub-set of SQL for defining materialized views. We do not currently support joins (although we intend to).
We support queries with and without aggregations. We do not support sub-queries. We support many of the standard MySQL
functions.

### Processors

*To be implemented*

Processors allows PranaDB to call out to custom processing logic, potentially hosted remotely, and implemented as gRPC
endpoints, AWS lambdas or in other ways. Essentially, this enables PranaDB to maintain custom materialized views where
the mapping from the input data to the output data is defined not by SQL, but by custom logic.

A processor is a table-like structure in PranaDB that takes as input some other table (a source, a materialized view, or
another processor) and then sends the data to an external defined function. This could be implemented in an external
gRPC endpoint, or as an AWS lambda, or elsewhere. The external function processes the data and returns the result to
PranaDB where it is persisted.

```
create processor fraud_approved_transactions (
    transaction_id varchar,
    customer_id bigint,
    transaction_time timestamp,
    amount decimal(10, 2),
    primary key (payment_id)
 ) from all_transactions
 with (  
    type = "grpc"
    properties = ( 
       address = "some_host:5678"
       encoding = "json"
    )
 )
```

This would take the data from the `all_transactions` source and for each input row call the specified gRPC endpoint.
Returned results would be stored in PranaDB. Processors can be queried using standard SQL or used as input to other
processors or materialized views.

### Secondary indexes

Sources and materialized views are sorted in storage by their primary key, making lookups or scans based on the primary
key efficient. However in some cases you may want to lookup or scan based on some other column(s) of the source.

For example, in the `all_transactions` source example from earlier, let's say we want to show transactions for a
particular customer:

```
select * from all_transactions where customer_id = 12345678
```

Without a secondary index on `customer_id` this would require a table scan of the `all_transactions` source - this could
be slow if there is a lot of data in the source.

#### Creating secondary indexes

You create a secondary index using the `create index` command.

```
create index idx_customer_id on all_transactions(customer_id);
```

Here, `idx_customer_id` is the name of the index that's being created. Unlike source or materialized view names which
are scoped to the schema, secondary index names are scoped to the source or materialized view on which they apply.

#### Dropping secondary indexes

You drop a secondary index by using the `drop index` command, you must specify the source or materialized view name too
as secondary index names are scoped to the source or materialized view

```
drop index idx_customer_id on all_transactions;
```

### Sinks

*To be implemented*

Sinks are the mechanism by which changes to materialized views flow back as events to external Kafka topics.

### Datatypes

PranaDB supports the following datatypes

* `varchar` (note: there is no max length to specify) - use this for string types
* `tinyint` - this is a signed integer with range -128 <= i <= 127 - typicall used for storing boolean types
* `int` - this is a signed integer with range -2147483648 <= i <= 2147483647
* `bigint` - this is a signed integer with range -2^63 <= i <= 2^63 - 1
* `decimal(p, s)` - this is an exact decimal type - just like the decimal type in MySQL. `p` is the "precision", this
  means the maximum number of digits in total, and `s` is the "scale", this means the number of digits to the right of
  the decimal point.
* `timestamp` - this is like the timestamp type in MySQL.

### Queries

There are two types of queries that can be executed in PranaDB.

Queries can be performed against any table-like structure in PranaDB - sources, materialized views or processors.

#### Pull queries

Pull queries are just like queries that you'd execute against a traditional relational database. You specify some SQL,
it's sent to the cluster and processed at that point in time and a sequence of rows is returned.

Pull queries can currently be executed using the command line client or using the gRPC API.

We currently support a subset of SQL in pull queries. We do not support joins, aggregations or sub-queries.

##### Prepared Statements

PranaDB supports prepared statements - this enables the SQL to be parsed once instead of every time it is executed.

Prepared statements are supported using the gRPC API.

#### Streaming queries

*To be implemented*

These stay open on the server and incrementally send back updates as the result of the query changes.

### Window functions

*To be implemented*

## Reference

### `use` statement

Switches to the schema identified by `schema_name`

`use <schema_name>`

### `create source` statement

Creates a source. A source ingests records from an external source (e.g. an Apache Kafka topic) into a table-like
structure.

```
create source <source_name>(
     <column1_name> <column1_datatype>,
     <column2_name> <column2_datatype>,
     ...,
     primary key (<pk_column_name>)
 ) with (
     brokername = "<broker_name>",
     topicname = "<topic_name",
     headerencoding = "<header_encoding>",
     keyencoding = "<key_encoding>",
     valueencoding = "<value_encoding>",
     columnselectors = (
         <column1_selector>,
         <column2_selector>,
         ...
     )
 );
```

`source_name` - the name of the source - it must be unique in the schema with respect to any other entity (source,
materialized view, sink or processor).
`columnx_name` - the name of column x - it must be unique in the source.
`columnx_datatype` - the datatype of the column x - one of `varchar`, `tinyint`, `int`, `bigint`, `decimal(p, s)`
or `timestamp`.

`broker_name` - the name of the Kafka broker to connect to. The names are defined along with the actual connection
settings in the PranaDB server configuration.
`topic_name` - the name of the Apache Kafka topic.

A Kafka message consists of

1. A set of headers - each header is a key, value pair. The key and value are both byte arrays.
2. An optional key - this is a byte array
3. The message value (the body of the message) this is the byte array.

In order to map an incoming Kafka message into a row in the source, PranaDB needs to know how interpret the data in the
headers, key and value. For example the key could be encoded as an UTF-8 string, and the value could be a JSON string or
an encoded protobuf.

`header_encoding`, `key_encoding` and `value_encoding` tell PranaDB how the headers, key and value are encoded. They can
take the following values:

* `json` - A JSON string
* `protobuf:<schema_name>` - An encoded protobuf. `schema_name` must contain the protobuf schema name.
  E.g. `com.squareup.cash.Payment`
* `stringbytes` - string encoded in UTF-8 format
* `float32be` - 32 bit float encoded in big endian format
* `float64be` - 64 bit float encoded in big endian format
* `int16be` - 16 bit int encoded in big endian format
* `int32be` - 32 bit int encoded in big endian format
* `int64be` - 64 bit int encoded in big endian format

The column selectors determine how each column in the row gets its value from the headers, key or value of the Kafka
message. We define a simple selector language for this that is similar to `jsonpath`. By default it's assumed that the
column value comes from the value (message body) of the Kafka message. You then construct the selector to extract the
appropriate field.

Let's say your message value is encoded as JSON. Here's an example (whitespace added for clarity):

```json
{
  "name": "bob's house",
  "rooms": [
    {
      "name": "living room",
      "length": 6,
      "width": 10
    },
    {
      "name": "kitchen",
      "length": 8,
      "width": 7
    }
  ]
}
```

The selector `name` would simply extract `bob's house`. The selector `rooms[0].length` would extract `6`
The selector `rooms[1].name` would extract `kitchen`

The same works for protobuf encoded messages.

If the column value needs to be extracted from headers then you use the special function `meta` to anchor the selector
language to the headers not the value. You then use the selector language as above to extract the data.

For example, if you wish to extract a column value from the a header called `my_key` of the Kafka message which is
encoded as a string you would use:

`meta("header").my_key`

If the header is encoded as JSON or protobuf and you wanted to extract a nested field you could do:

`meta("header").my_key.my_nested_field_name`

Similarly if you want to extract data from the key of the Kafka message you use `meta("key")`.

For extracting the timestamp of the Kafka message you use `meta("timestamp")`.

### `drop source` statement

Drops a source

`drop source <source_name>`

This will fail if there are any child entities (materialized views, sinks or processors) - they must be dropped first.

### `create sink` statement

*Not yet implemented*

### `drop sink` statement

*Not yet implemented*

### `create materialized view` statement

Creates a materialized view.

`create materialized view <name> as <query>`

Creates a materialized view with name `name` which is defined by the query `query`.

`name` must be unique across all entities in the schema.

### `drop materialized view` statement

Drops a materialized view - deleting all it's data.

`drop materialized view <name>`

### `create index` statement

Creates a secondary index on an entity such as a source or materialized view.

`create index <name> on <entity_name>(<column1>, <column2>, ...)`

### `drop index` statement

Drops an index

`drop index <name> on <entity_name>`

### `show tables` statement

Shows the tables in the current schema - tables include sources and materialized views;

`show tables`

### Server configuration

A configuration file is used to configure a PranaDB server. It is specified on the command line when running the PranaDB
executable using the `config` parameter.

```shell
pranadb --config cfg/example.conf --node-id 1
```

There's an example config file `cfg/example.conf` in the PranaDB GitHub repository. This can act as a good starting
point when configuring your PranaDB server. The example config works out of the box for demos when all PranaDB nodes are
running on your local machine. The addresses in the config file will need to be adapted if you are running on separate
hosts.

Typically you will use the exact same PranaDB configuration file for every node in your PranaDB cluster. Each PranaDB
server is identified by a `node-id` which is an integer from `0 .. n - 1`. Where `n` is the number of nodes in the
cluster. The `node-id` is *not* specified in the server configuration file, it is provided on the command line, allowing
us to use the same config files on each cluster. This makes things simpler when, say, deploying PranaDB in a Kubernetes
cluster.

PranaDB uses the [Hashicorp Configuration Language](https://www.terraform.io/language/syntax/configuration) (HCL) for
it's config file - (this is the one used by Terraform).

The following configuration parameters are commonly used:

* `cluster-id` - This uniquely identifies a PranaDB cluster. Every node in a particular PranaDB cluster must have the
  same `cluster-id`
  or different clusters can interfere with each other. Different clusters must have different values for `cluster-id`.
  It must be a positive integer.
* `raft-addresses` - The nodes in the PranaDB cluster use Raft groups for various purposes such as replicating writes
  for reliability. This parameter must contain a list of addresses (host:port) for each node in the PranaDB cluster. The
  address for node `i` must be at index
  `i` in the list. These addresses need to be accessible from each PranaDB node but don't need to be accessible from
  elsewhere.
* `notif-listen-addresses` - Each PranaDB broadcasts notifications to other nodes for internal use. These addresses
  define the addresses at which each node listens for notifications. This parameter must contain a list of addresses (
  host:port) for each node in the PranaDB cluster. The address for node `i` must be at index
  `i` in the list. These addresses need to be accessible from each PranaDB node but don't need to be accessible from
  elsewhere.
* `api-server-listen-addresses` - Each PranaDB server can host a gRPC server. This is currently used by the client when
  making connections to a PranaDB server in order to execute statements. This parameter must contain a list of
  addresses (host:port) for each node in the PranaDB cluster. The address for node `i` must be at index
  `i` in the list. These addresses need to be accessible from each PranaDB node and also need to be accessible from
  clients.
* `num-shards` - Every piece of data in a PranaDB cluster lives in a shard. Typically there are an order of magnitude
  times as many shards as nodes in the cluster. Currently the number of shards in the cluster is fixed and must not be
  changed once the cluster has been used.
* `replication-factor` - This determines how many replicas there are of every shard. All data in PranaDB is replicated
  multiple times for better durability. The minimum size for this parameter is `3`
* `data-dir` - This specifes the location where all PranaDB data will live. PranaDB will create sub-directories within
  this directory for different types of data.
* `kafka-brokers` - This specifies a mapping between a Kafka broker name and the config for connecting to that Kafka
  broker. It used in sources when connecting to Kafka brokers to ingest data. The name is an arbitrary unique string and
  is used in the source configuration to specify a broker to use. Typically many different sources will use the same
  Kafka broker so this saves the source configuration having to reproduce the Kafka broker connection information every
  time. Also, in a managed set-up you may not want your users creating sources to connect to arbitrary Kafka brokers.
  Each kafka broker in the map has a parameter `client-type` which must currently always be `2`. It also has a map of
  properties which are passed to the Kafka client when connecting. We currently use
  the [Confluent Kafka Go client](https://github.com/confluentinc/confluent-kafka-go
  . At a minimum, the property `bootstrap.servers` must be specified with a comma separated list of addresses (host:
  port) of the Kafka brokers.
* `log-level` one of `[trace|debug|info|warn|error]` - this determines the logging level for PranaDB. Logs are written
  to stdout.

### The gRPC API

PranaDB provides a [gRPC API](../protos/squareup/cash/pranadb/service/v1/service.proto) for access from applications.
This is also used by the PranaDB command line client.

The API is essentially very simple - you create a session, then you pass statements as strings to PranaDB and it returns
results. The statements can be any statements that you can type at the PranaDB command line.



