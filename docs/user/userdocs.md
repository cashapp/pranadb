# PranaDB User Documentation

## What is PranaDB?

PranaDB is a *streaming database*.

PranaDB ingests data streams from external sources - e.g. Apache Kafka topics, and allows you to define computations,
usually expressed in standard SQL, over those streams. The results of those computations are stored persistently. We
call these *materialized views* and they update automatically and continuously as new data arrives.

Materialized views are typically defined using standard SQL but we also want to allow custom, user provided, lambda
functions to be usable in materialized views in the future. You access the data in those materialized views by executing
queries, also using standard SQL, just as you would with a traditional relational database.

PranaDB is a real distributed database, it is not simply an in-memory cache, and is designed from the beginning to scale
horizontally to effectively support views with very large amounts of data. Once ingested, it owns the data, it does not
use Apache Kafka to store intermediate state.

Going ahead we also want to make it possible to publish events *directly* to PranaDB and to be able to consume them as
stream, in much the same as you would with Apache Kafka. PranaDB then becomes a hybrid of an event streaming platform (
such as Kafka) and a relational database (such as MySQL).

Imagine a Kafka where you can query events in your topics using SQL and where you can make persistent projections of
those events. With PranaDB the tricky problems of consistency, scalability and availability for you, and you just use
SQL to define your views or retrieve data, or use lambdas, written in your programming language of choice to express
custom computations.

That's the vision behind PranaDB and we believe it's a very powerful proposition.

### Example

Here's a quick example.

We have a Kafka topic which contains transaction events for all customers. We create a *source*. A PranaDB source is
like a database table where the data gets filled from a Kafka topic:

```sql
create
source all_transactions(
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

```sql
select *
from all_transactions
where customer_id = 12345678;
```

We can now create a materialized view that maintains each customer's current balance.

```sql
create
materialized view customer_balances as
select customer_id, sum(amount) as total
from all_transactions
group by customer_id
```

You can now query that from your app to get the customer's current balance

```sql
select total
from customer_balances
where customer_id = 12345678
```

## Running the server

_The following assumes you have checked out the GitHub project and you have golang installed. You can then build and run
the server just using `go run ...`. This is a convenient way to play around with the project. Alternatively you could
build the Prana executables first and execute them directly anywhere without needing go to be installed._

PranaDB requires a minimum of three nodes to form a cluster - data is replicated to three nodes by default.

To run the server:

```shell
pranadb> pranadb --config cfg/example.conf --node-id 0
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
pranadb> go run cmd/prana/main.go shell
```

By default, the client will attempt to connect to a PranaDB instance running on `localhost:6584`, if your PranaDB
instance is running on a different host:port then you can specify this on the command line:

```shell
pranadb> go run cmd/prana/main.go shell --addr myhost:7654
```

## The PranaDB mental model

The PranaDB mental model is very simple and should be second nature to you if you've had experience with relational
databases. The SQL used in queries is standard and we don't introduce any new or confusing syntax. The language used for
creating or dropping PranaDB entities such as sources and materialized views is very similar to the syntax you'd use
when creating or dropping entities in a relational database such as a table.

### Schemas

### Sources

PranaDB ingests data from external feeds such as Kafka topics into entities called _sources_. You can think of a source
as basically like a database table that you'd find in any relational database - it has rows and columns, and the columns
can have different types. Just like a relational database table you can query it by executing a query expressed using
SQL.

The main difference between a relational database table and a PranaDB source is that you can directly `insert`, `update`
or `delete` rows in it from a client. With a PranaDB you can't do that. The only way that rows in a PranaDB source get
inserted, updated or deleted is by events being consumed from the Kafka topic and being translated to inserts/updates or
deletes in the source.

#### Creating a source

You create a source with a `create source` statement. This is quite similar to a `create table` statement that you're
probably already familiar with. In the `create source` statement you provide the name of the source and the name and
types of the columns.

You also need to provide a primary key for the source. Incoming data with the same value of the primary key *upserts*
data in the source (i.e either inserts or updates any existing data).

Data is indexed on the primary key which makes queries which lookup or scan ranges of the primary key efficient. You can
also create secondary indexes on other columns of the source. This can help avoid scanning the entire source for queries
that lookup values or ranges of columns other than the primary key.

Where a `create source` statement differs from a relational database `create table` statement is you also need to tell
PranaDB *where* to get the data from and how to decode the incoming data, and how parts of the incoming message map to
columns of the source.

Here's an example `create source` statement

```sql
create
source all_transactions(
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

The source data is ingested from a Kafka topic called `transactions`, in a Kafka broker named `testbroker`.
(The config for the broker is defined in the server configuration file). The rest of the configuration describes how the
data is encoded in the incoming Kafka message and how to retrieve the column values from the Kafka message.

For a full description of how to configure a source, please consult the source reference.

#### Dropping a source

You drop a source with a `drop source` statement. For example:

```sql
drop
source all_transactions;
```

Dropping a source deletes the source from PranaDB including all the data it contains. Dropping a source is irreversible.

You won't be able to drop a source if it has child materialized views. You'll have to drop any children first.

### Materialized views

cascades

### Datatypes

#### SQL supported in materialized views

### Queries

### Secondary indexes

### Sinks

*TODO - not currently implemented*

Sinks are the mechanism by which changes to materialized views flow back as events to external Kafka topics.

## Clustering

Sharding

## Replication

Raft

## Reference

### `use` statement

### `create source` statement

### `drop source` statement

### `create sink` statement

### `drop sink` statement

### `create materialized view` statement

### `drop materialized view` statement

### `create index` statement

### `drop index` statement

### `show tables` statement

### Query syntax

### Server configuration

## Frequently asked questions

### What's the current status of PranaDB?

PranaDB is currently a technical preview. It's already pretty solid, but we have many more features yet to add, and we
haven't done much performance optimisation yet.

### How does PranaDB compare with other products on the market

Unlike other systems which have some similarities, Prana:

* Is not an in-memory a cache. It is designed from the beginning to be a true horizontally scalable distributed database
  and support very large persistent views.
* Uses a standard SQL "tables only" mental model that is familiar to anyone who has used a relational database.
* Once ingested, Prana owns the data. It does not delegate internal intermediate storage to an event streaming platform,
  such as Apache Kafka.
