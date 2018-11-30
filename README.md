# Replication machine

Realisation replication between databases. Support: cockroach, postgres

## Launching

open file `config.exs`:

* ```[src]``` - source database. Required to transfer: hostname, username, password, database and port

* ```[dest]``` - destination database. Required to transfer: type database([postgres/cockroach]), hostname, username, password, database and port

* ```[limit]``` - number of entries per serving for insert

* ```[select_table_and_unique]``` - pair table name (key), unique table fields (value). The order is important to observe the order of insertion of tables depending on their references to each other by a foreign key!

For example, postgres to postgres:

```elixir
src: [{:hostname, "localhost"}, {:username, "postgres"}, {:password, "postgres"}, {:database, "pusher_prod"}, {:port, 5432}],
dest: [{:type, "postgres"}, {:hostname, "localhost"}, {:username, "postgres"}, {:password, "postgres"}, {:database, "pusher_prod_dest"}, {:port, 5432}],
limit: 3,
select_table_and_unique: [{:feed_blocks, "id"}, {:sessions, "id"}]
```
