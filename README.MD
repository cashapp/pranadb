# PranaDB

PranaDB is a streaming database...

## Quick demo

Execute the demo from the Prana project base directory.

First open a terminal and start Kafka and Zookeeper - the script will download it the first time.

```
scripts/start_kafka.sh
```

Once Kafka is running (you can see this from the output), run this script to create a topic called `payments`:

```
scripts/create_topics.sh
```

Now we're going to start three Prana servers. Three is the minimum size for a cluster.

Open three terminals.

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

Wait for all the servers to report they are started.

Now we're going to start a Prana CLI. Open another terminal and type:

```
go run cmd/prana/main.go shell
```

The CLI should start and give you a prompt like this:

```
pranadb>
```

First we tell Prana that we want to use the `test` schema - this is the schema in which we're going to work.

```
pranadb> use test;
0 rows returned
```

Now let's create a source. A source is like a table in a relational database, but it gets it's data from a Kafka topic.
When creating the source we not only need to tell it what the columns are, as we do with a relational database table. We
also have to tell it from which topic to get the data, and how to map the data in the Kafka record to the columns of the
table.

We'll create a source called payments which represents customer payments. It's primary key is `payment_id` and it has
fields representing the customer id, payment type, amount etc. Prana will get the data for the `payments` source from
the `payments` topic, that we created earlier. This topic has the payment information encoded as JSON in the Kafka
message.

Try creating the source, by copy and pasting the following into the Prana CLI:

```
pranadb> create source payments(
             payment_id varchar,
             customer_id bigint,
             payment_time timestamp,
             amount decimal(10, 2),
             payment_type varchar,
             currency varchar,
             fraud_score double,
             primary key (payment_id)
         ) with (
             brokername = "testbroker",
             topicname = "payments",
             headerencoding = "stringbytes",
             keyencoding = "stringbytes",
             valueencoding = "json",
             columnselectors = (
                 "k",
                 "v.customer_id",
                 "t",
                 "v.amount",
                 "v.payment_type",
                 "v.currency",
                 "h.fraud_score"
             )
             properties = ()
         );
0 rows returned
pranadb>
```

Let's see if there are any rows in the source:

```
pranadb> select * from payments;
|payment_id|customer_id|payment_time|amount|payment_type|currency|fraud_score|
0 rows returned
```

No surprise - there's no data in the topic yet!

Let's add some messages to the Kafka topic. We will use a simple utility which just uses a Kafka client to generate some
payment messages and send them to the `payments` topic.

We will send 1000 messages starting at id 0 through 999:

```
go run cmd/msggen/main.go --generator-name payments --topic-name payments --partitions 25 --delay 0 --num-messages 1000 --index-start 0 --kafka-properties "bootstrap.servers"="localhost:9092"
2021-08-30 16:27:56.463719 I | Messages sent ok
```

Now let's look in the source again:

```
pranadb> select * from payments order by payment_id;
|payment_id|customer_id|payment_time|amount|payment_type|currency|fraud_score|
|payment000000|0|2100-04-12 09:00:00.000000|21126.00|btc|gbp|0.81|
|payment000001|1|2100-04-12 09:00:00.000000|66927.00|p2p|usd|0.96|
|payment000002|2|2100-04-12 09:00:00.000000|67979.70|other|eur|0.61|
|payment000003|3|2100-04-12 09:00:00.000000|86160.30|btc|aud|0.13|
|payment000004|4|2100-04-12 09:00:00.000000|67122.50|p2p|gbp|0.36|
|payment000005|5|2100-04-12 09:00:00.000000|34629.70|other|usd|0.99|
... snip ...
|payment000995|9|2100-04-12 09:00:00.000000|51833.30|other|aud|0.11|
|payment000996|10|2100-04-12 09:00:00.000000|21807.00|btc|gbp|0.19|
|payment000997|11|2100-04-12 09:00:00.000000|34375.70|p2p|usd|0.01|
|payment000998|12|2100-04-12 09:00:00.000000|43120.80|other|eur|0.71|
|payment000999|13|2100-04-12 09:00:00.000000|69681.00|btc|aud|0.01|
1000 rows returned
```

The rows have arrived.

Now let's create a materialized view which maintains only big payments where amount > 90000

```
pranadb> create materialized view big_payments as select * from payments where amount > 90000;
0 rows returned
```

And see what rows are there:

```
pranadb> select * from big_payments order by amount;
|payment_id|customer_id|payment_time|amount|payment_type|currency|fraud_score|
|payment000986|0|2100-04-12 09:00:00.000000|90309.60|other|eur|0.25|
|payment000931|13|2100-04-12 09:00:00.000000|90426.80|p2p|aud|0.74|
|payment000563|2|2100-04-12 09:00:00.000000|90614.80|other|aud|0.5|
|payment000037|3|2100-04-12 09:00:00.000000|90660.10|p2p|usd|0.42|
|payment000924|6|2100-04-12 09:00:00.000000|90680.10|btc|gbp|0.14|
... snip ...
|payment000547|3|2100-04-12 09:00:00.000000|99569.20|p2p|aud|0.7|
|payment000104|2|2100-04-12 09:00:00.000000|99636.10|other|gbp|0.78|
|payment000882|15|2100-04-12 09:00:00.000000|99734.50|btc|eur|0.09|
|payment000235|14|2100-04-12 09:00:00.000000|99793.20|p2p|aud|0.07|
|payment000727|13|2100-04-12 09:00:00.000000|99920.70|p2p|aud|0.48|
107 rows returned
```

Cool, it only selects the rows we want.

Let's create a second materialized view based on the first materialized view which only selects BitCoin payments:

```
pranadb> create materialized view big_btc_payments as select * from big_payments where payment_type='btc';
0 rows returned
```

What's it got in it?

```
pranadb> select * from big_btc_payments;
|payment_id|customer_id|payment_time|amount|payment_type|currency|fraud_score|
|payment000741|10|2100-04-12 09:00:00.000000|98400.80|btc|usd|0.06|
|payment000207|3|2100-04-12 09:00:00.000000|91815.40|btc|aud|0.7|
|payment000582|4|2100-04-12 09:00:00.000000|92356.30|btc|eur|0.43|
|payment000882|15|2100-04-12 09:00:00.000000|99734.50|btc|eur|0.09|
|payment000405|14|2100-04-12 09:00:00.000000|93870.10|btc|usd|0.33|
|payment000288|16|2100-04-12 09:00:00.000000|93677.50|btc|gbp|0.77|
|payment000564|3|2100-04-12 09:00:00.000000|94640.20|btc|gbp|0.03|
|payment000735|4|2100-04-12 09:00:00.000000|92652.30|btc|aud|0.55|
|payment000771|6|2100-04-12 09:00:00.000000|98937.70|btc|aud|0.83|
... snip ...
|payment000438|13|2100-04-12 09:00:00.000000|97277.70|btc|eur|0.1|
|payment000522|12|2100-04-12 09:00:00.000000|97125.20|btc|eur|0.57|
|payment000348|8|2100-04-12 09:00:00.000000|91733.40|btc|gbp|0.38|
|payment000903|2|2100-04-12 09:00:00.000000|92714.90|btc|aud|0.63|
|payment000660|14|2100-04-12 09:00:00.000000|92786.00|btc|gbp|0.6|
36 rows returned
```

Just the big BTC payments as we wanted.

What happens when more data arrives on the topic? Let's add another 10000 messages starting at id 1000. This time we're
going to introduce a delay of 10ms between each message send, so you can execute the queries and see how the records in
the source and the two materialized views change over time.

```
go run cmd/msggen/main.go --generator-name payments --topic-name payments --partitions 25 --delay 10ms --num-messages 10000 --index-start 1000 --kafka-properties "bootstrap.servers"="localhost:9092"
```

Now try executing the queries again to see what happens. You can use the up and down cursor keys to select a previous
query from history:

```
select * from payments;

select * from big_payments;

select * from big_btc_payments;
```

You can see that the state of the source and materialized views automatically updates as more data arrives from Kafka

Ok, let's create a materialized view that maintains an aggregated view of a customer's account. It will show a
customer's number of payments, total of payments, minimum and maximum payment, for each payment type.

|payment_id|customer_id|payment_time|amount|payment_type|currency|fraud_score|

```
pranadb> create materialized view customer_balances as
select customer_id, payment_type, count(*), sum(amount), min(amount), max(amount) from payments group by customer_id, payment_type;
0 rows returned
```

Let's see what's in it:

````
pranadb> select * from customer_balances;

pranadb> select * from customer_balances;
|customer_id|payment_type|count(*)|sum(amount)|min(amount)|max(amount)|
|3|p2p|19|964496.800000000000000000000000000000|2156.900000000000000000000000000000|96137.300000000000000000000000000000|
|10|p2p|20|900814.800000000000000000000000000000|6646.100000000000000000000000000000|89693.200000000000000000000000000000|
|12|btc|20|905986.500000000000000000000000000000|9843.100000000000000000000000000000|97955.800000000000000000000000000000|
... snip ...
|11|other|20|1108386.700000000000000000000000000000|4462.800000000000000000000000000000|95029.100000000000000000000000000000|
|16|other|19|891111.500000000000000000000000000000|6130.200000000000000000000000000000|94976.600000000000000000000000000000|
|2|btc|19|1021175.500000000000000000000000000000|1373.600000000000000000000000000000|89910.800000000000000000000000000000|
|2|p2p|20|1204245.100000000000000000000000000000|9104.200000000000000000000000000000|97201.800000000000000000000000000000|
|6|btc|20|1174769.600000000000000000000000000000|21136.300000000000000000000000000000|98274.900000000000000000000000000000|
|6|other|20|1177876.200000000000000000000000000000|6713.400000000000000000000000000000|99725.400000000000000000000000000000|
51 rows returned
````

Let's view it for a particular customer:

````
pranadb> select * from customer_balances where customer_id=12;
|customer_id|payment_type|count(*)|sum(amount)|min(amount)|max(amount)|
|12|btc|78|3695844.600000000000000000000000000000|1112.400000000000000000000000000000|97955.800000000000000000000000000000|
|12|p2p|78|4360112.800000000000000000000000000000|3139.800000000000000000000000000000|99498.300000000000000000000000000000|
|12|other|78|4053767.300000000000000000000000000000|3176.500000000000000000000000000000|96360.400000000000000000000000000000|
3 rows returned
````

As you can see the count, sum, min and max are broken down by payment type.

Let's add some more payments to the topic

````
go run cmd/msggen/main.go --generator-name payments --topic-name payments --partitions 25 --delay 0 --num-messages 2000 --index-start 2000 --kafka-properties "bootstrap.servers"="localhost:9092"
````

The aggregate materialized view has now been updated

````
pranadb> select * from customer_balances where customer_id=12;
|customer_id|payment_type|count(*)|sum(amount)|min(amount)|max(amount)|
|12|btc|118|5676632.600000000000000000000000000000|1112.400000000000000000000000000000|99927.100000000000000000000000000000|
|12|p2p|117|6306247.700000000000000000000000000000|2834.400000000000000000000000000000|99498.300000000000000000000000000000|
|12|other|117|5811145.800000000000000000000000000000|3176.500000000000000000000000000000|99229.500000000000000000000000000000|
3 rows returned
````