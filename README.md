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
Because this is just a test, we decided to hard code this three column names in the code so that we dont have to create all the logic behind dynamic column names with different types. It is hard coded for columns `primary_id`, `secondary_id` and `body`. Then we can create the table using the query

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

For the [Storage Engine](https://github.com/NeowayLabs/ClickHouse/blob/fulltext-21.3/src/Storages/StorageTantivy.cpp) it has to be able to recive data from the INSERT query and index into tantivy. For the SELECT queries we need to push the full text WHERE clause to tantivy and create a Clickhouse column with the result.

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
 TODO

## Alternatives
 TODO
- Use [data skip index](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-data_skipping-indexes)
- Implement [inverted index](https://hannes.muehleisen.org/SIGIR2014-column-stores-ir-prototyping.pdf) on SQL
- Tantivy
