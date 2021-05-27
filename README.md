[![ClickHouse — open source distributed column-oriented DBMS](https://github.com/ClickHouse/ClickHouse/raw/master/website/images/logo-400x240.png)](https://clickhouse.tech)

ClickHouse® is an open-source column-oriented database management system that allows generating analytical data reports in real time.

## Useful Links

* [Official website](https://clickhouse.tech/) has quick high-level overview of ClickHouse on main page.
* [Tutorial](https://clickhouse.tech/docs/en/getting_started/tutorial/) shows how to set up and query small ClickHouse cluster.
* [Documentation](https://clickhouse.tech/docs/en/) provides more in-depth information.
* [YouTube channel](https://www.youtube.com/c/ClickHouseDB) has a lot of content about ClickHouse in video format.
* [Slack](https://join.slack.com/t/clickhousedb/shared_invite/zt-ly9m4w1x-6j7x5Ts_pQZqrctAbRZ3cg) and [Telegram](https://telegram.me/clickhouse_en) allow to chat with ClickHouse users in real-time.
* [Blog](https://clickhouse.yandex/blog/en/) contains various ClickHouse-related articles, as well as announcements and reports about events.
* [Code Browser](https://clickhouse.tech/codebrowser/html_report/ClickHouse/index.html) with syntax highlight and navigation.
* [Contacts](https://clickhouse.tech/#contacts) can help to get your questions answered if there are any.
* You can also [fill this form](https://clickhouse.tech/#meet) to meet Yandex ClickHouse team in person.


## Neoway Research

This branch is part of a research where we implemented a proof of concept for full text search using [ClickHouse](https://github.com/ClickHouse/ClickHouse) and [Tantivy](https://github.com/tantivy-search/tantivy).

Tantivy is a full text search engine library written in Rust.

The implementation consists in creating the tantivy storage engine and tantivy SQL function.
Because this is just a test, we decided to hard code this three column names in the code so that we don't have to create all the logic behind dynamic column names with different types. It is hard-coded for columns `primary_id`, `secondary_id` and `body`. Then we can create the table using the query

```sql
CREATE TABLE fulltext_table
(
    primary_id UInt64,
    secondary_id UInt64,
    body String
)
ENGINE = Tantivy('/var/lib/clickhouse/tantivy/fulltext_table')
-- Tantivy engine takes as parameter a path to save the data.
```

For the [Storage Engine](https://github.com/NeowayLabs/ClickHouse/blob/fulltext-21.3/src/Storages/StorageTantivy.cpp) it has to be able to receive data from the INSERT query and index into tantivy. For the SELECT queries we need to push the full text WHERE clause to tantivy and create a Clickhouse column with the result.

Because the full text search query needs to be sent to tantivy we created an SQL function named tantivy, so the syntax for making queries is the following
```sql
SELECT primary_id
FROM fulltext_table
WHERE tantivy('full text query here')
```
The `tantivy` SQL function doesn't return anything and has no logic inside. Its only purpose is to validade the input and generate the `ASTSelectQuery`.
Inside the storage engine we take the AST parameters and push the query to the Rust implementation inside the folder [contrib/tantivysearch](https://github.com/NeowayLabs/ClickHouse/tree/fulltext-21.3/contrib/tantivysearch).

When data is indexed in tantivy it needs to be commited. That's an expensive job to do every insert so we decided to call it when optimize table is called
```sql
OPTIMIZE TABLE fulltext_table FINAL
```
After the optimization the data is available for queries.

## Results
We inserted 39 million texts with an average of 4895 characters, also all the texts were unique. Our testing machine is a n2d-standard-16, 16 CPU, 62.8G Mem, 2 Local SSD 375 GB in RAID 0, on Google Cloud.

In our implementation we were not interested in retrieving the actual text from the search result. That means we chose to return only the ID columns and don't return the text. It would be easy to return the text, but for our use case we just want to have statistics on the data. An example would be to answer how many rows match with the phrase 'covid 19' ? The result for that is a query that runs at the same speed tantivy would run with a little increment of time to copy the result to a Clickhouse column. For the majority of searches we could get the result in milliseconds. Queries using OR operator and matching almost all the texts were slower and could time more than 1 second.

Another use case is that we have a table with dozens of columns that is related to our fulltext_table by an ID. So we would have a query like this
```sql
SELECT *
FROM a_very_big_table
WHERE
    -- many_filters_here
    AND primary_id IN (
        SELECT primary_id
        FROM fulltext_table
        WHERE tantivy('full text query here')
    )
```
Also we wanted to do many different queries, all with the same text filter, and running in parallel. Instead of doing the same query on tantivy at the same time, with the same result, we implemented a concurrent bounded cache mechanism that we can set a TTL and perform a single computation for multiple parallel queries on the same input resolving the same result to all once done. We noticed that the speed of those queries were fast making this solution very promising.


## Alternatives
Other alternatives to this is to use [data skipping indexes](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-data_skipping-indexes) or implement something akin to an [inverted index](https://hannes.muehleisen.org/SIGIR2014-column-stores-ir-prototyping.pdf) on SQL directly.

Data skipping indexes requires a lot of parameter tuning and it is very tricky to make they work with the SQL functions. Even with all that tuning we got very poor performance.

Inverted index is an interesting solution, but it is very complex to implement and requires an external tokenizer and big complicated queries to search the data. The performance is better than data skipping indexes but still too slow for a real scenario.
