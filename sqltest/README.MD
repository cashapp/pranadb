# SQLTest

SQLTest is a test tool that makes it easy to write tests for Prana.

You specify your test a script to be run, together with a text file containing expected output, and another text file
containing test data

A test with name `<test_name>` is made up of three files:

## The script file

This is named `<test_name>_test_script.txt`

***Important note! Every statement or comment in a script file MUST end in a semicolon, or it will not be parsed
correctly***

It contains a sequence of SQL statements, just like the ones you would type into a Prana CLI.

Statements can be DDL statements such as `create source...`, `create materialized view...`, `drop source...`,
`drop materialized view...`, or queries such as `select * from some_table`.

SQLTest runs your script, collects the output and compares it to the expected output

Test scripts can also contain SQL comments which begin with `--`.

### Special directives

We define some special comments which do special things when they are encountered. This allows us to do lots of powerful
things in tests. Here are the currently supported special comment directives:

* `--load_data <dataset_name>;`. This loads the dataset with the specified name
* `--repeat <n>;` This repeats the test `n` times in a loop. Very useful for reproducing non-deterministic test
  failures. If there's a repeat directive, it must be the first line in a script.
* `--close session;` This closes the current SQL session at that point. A new session will then be created for the next
  statement. Used for testing scope of prepared statements.

### Note on prepared statements

We also support some special statements that can be used for testing prepared statements.

To create a prepared statement for a query:

```
prepare select * from foo order by bar;
```

This will return the prepared statement id.

Then to execute a prepared statement:

```
execute 3 hello 23.2
```

The first argument is the id of the prepared statement. The remaining arguments are the arguments for the prepared
statement. They are space delimited.

## The expected output file

This is named `<test_name>_test_out.txt`.

It contains the expected output of the test. SQLTest compares the actual output of running the script with the expected
output and if they are the same, the test passes, otherwise it fails.

If the test fails, the actual output will be written to stdout, so you can see what happened.

## The test data file

This is named `<test_name>_test_data.txt`.

A test data file can contain multiple data-sets. Each dataset is a set of rows, in CSV format, with one row per line.
Each data-set starts with a descriptor line, like the following:

```
dataset:my_data_set1 my_source_1
```

This says, here is a dataset called `my_data_set1` and it gets inserted into a source with name `my_source_1`

Datasets are inserted into sources at points in the script where a special `load-data` directive in a SQL comment is
encountered, e.g:

```
--load data dataset_1;
```

When the script encounters this line, it loads the data from the dataset with that name into the source described in the
dataset descriptor. It then waits for the data to be fully propoagated and any cascading MVs across the cluster to be
updated before proceeding.

## Running tests

By default tests run both against a fake single node cluster and a real three node cluster.

You can run these individually by running the test cases `TestSqlFakeCluster` and `TestSqlFakeCluster` in SQLTest.

## Tests that clean-up

SQLTest will check that the cluster is in a clean state at the end of every test run - that means there are no sources
or materialized views, no data in user tables in storage, and no sessions in memory.

That means you must drop all sources and materialized views that you create at the end of each test script.

### Running an individual test

You can run an individual test or subset of tests which start with the same prefix by setting the variable `TestPrefix`
in SQLTest
