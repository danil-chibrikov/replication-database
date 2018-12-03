defmodule CockroachDb.Application do

  def start(_type, _args) do
    Postgrex.Types.define(Postgrex.DefaultTypes, [], json: Poison)
    app_env = Application.get_all_env(:cockroach_db)
              |> Enum.into(%{})
    src = Map.get(app_env, :src)
    src_config = [{:hostname, Keyword.get(src, :hostname)},
                  {:username, Keyword.get(src, :username)},
                  {:password, Keyword.get(src, :password)},
                  {:database, Keyword.get(src, :database)},
                  {:port, Keyword.get(src, :port)},
                  {:extensions, [{Postgrex.Extensions.JSON}]},
                  {:name, :source}]
    dest = Map.get(app_env, :dest)
    dest_config = [{:hostname, Keyword.get(dest, :hostname)},
                   {:username, Keyword.get(dest, :username)},
                   {:password, Keyword.get(dest, :password)},
                   {:database, Keyword.get(dest, :database)},
                   {:port, Keyword.get(dest, :port)},
                   {:extensions, [{Postgrex.Extensions.JSON}]},
                   {:name, :destination}]
    children = [
      %{
        id: "source",
        start: {Postgrex, :start_link, [src_config]}
      },
      %{
        id: "destination",
        start: {Postgrex, :start_link, [dest_config]}
      },
      %{
        id: "worker",
        start: {CockroachDb.Worker, :start_link, [name: :worker]}
      }
    ]

    opts = [strategy: :one_for_one]
    Supervisor.start_link(children, opts)
  end
end
