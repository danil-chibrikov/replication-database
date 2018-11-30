use Mix.Config

config :cockroach_db,
src: [{:hostname, "localhost"}, {:username, "postgres"}, {:password, "postgres"}, {:database, "pusher_prod"}, {:port, 5432}],
dest: [{:type, "postgres"}, {:hostname, "localhost"}, {:username, "postgres"}, {:password, "postgres"}, {:database, "pusher_prod_dest"}, {:port, 5432}],
limit: 3,
select_table_and_unique: [{:feed_blocks, "id"}, {:sessions, "id"}]
