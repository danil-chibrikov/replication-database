defmodule CockroachDb.Application do

  def start(_type, _args) do

    Postgrex.Types.define(Postgrex.DefaultTypes, [], json: Poison)
    recursive_loop
    Supervisor.start_link([], strategy: :one_for_one)
  end

  def recursive_loop() do
    app_env = Application.get_all_env(:cockroach_db)
              |> Enum.into(%{})

    src = get_config_value(app_env, :src)
    dest = get_config_value(app_env, :dest)

    {:ok, select_pid} = Postgrex.start_link([{:hostname, Keyword.get(src, :hostname)},
                                             {:username, Keyword.get(src, :username)},
                                             {:password, Keyword.get(src, :password)},
                                             {:database, Keyword.get(src, :database)},
                                             {:port, Keyword.get(src, :port)},
                                             {:extensions, [{Postgrex.Extensions.JSON}]}])
    {:ok, insert_pid} = Postgrex.start_link([{:hostname, Keyword.get(dest, :hostname)},
                                             {:username, Keyword.get(dest, :username)},
                                             {:password, Keyword.get(dest, :password)},
                                             {:database, Keyword.get(dest, :database)},
                                             {:port, Keyword.get(dest, :port)},
                                             {:extensions, [{Postgrex.Extensions.JSON}]}])
    limit = get_config_value(app_env, :limit)
    select_name = get_config_value(app_env, :select_table_and_unique)
    type = Keyword.get(dest, :type)
    Enum.map(select_name, fn {table_name, uniques} ->
      select_src(select_pid, insert_pid, limit, Atom.to_string(table_name), uniques, type)
    end)
    Process.exit(insert_pid, :shutdown)
    Process.exit(select_pid, :shutdown)
    Process.sleep(5000)
    #recursive_loop
  end

  def get_config_value(list, atom) do
    Map.get(list, atom)
  end

  def select_src(select_pid, insert_pid, limit, table_name, uniques, type) do
    help_map_db = Postgrex.query!(select_pid,
      "SELECT * FROM #{table_name} ORDER BY updated_at LIMIT #{limit}"|> IO.inspect, [])
    |> get_zip
    updated_at_list = Keyword.get(help_map_db, String.to_atom("updated_at"))
    case length(updated_at_list) do
      0 ->
        IO.puts "Empty table"
      _ ->
        last_timestamp = Enum.fetch!(updated_at_list, -1)
                         |> NaiveDateTime.to_string
        next_map_db = Postgrex.query!(select_pid,
          "SELECT * FROM #{table_name} WHERE updated_at <= \'#{last_timestamp}\' ORDER BY updated_at" |> IO.inspect, [])
        |> get_zip
        insert_query = format_insert_query(next_map_db, table_name, uniques)
        case Postgrex.transaction(insert_pid, fn conn ->
          Postgrex.query!(conn, insert_query, [])
        end) do
          {:ok, _} ->
            case type do
              "postgres" ->
                %Postgrex.Result{rows: [[max_id]]} = Postgrex.query!(insert_pid, "SELECT MAX(#{uniques})FROM #{table_name}" |> IO.inspect, [])
                Postgrex.query!(insert_pid, "ALTER SEQUENCE #{table_name}_#{uniques}_seq RESTART with #{(max_id+1)}", [])
              "cockroach" ->
                %Postgrex.Result{rows: [[max_id]]} = Postgrex.query!(insert_pid, "SELECT MAX(#{uniques})FROM #{table_name}" |> IO.inspect, [])
                Postgrex.query!(insert_pid, "SELECT setval('#{table_name}_seq',#{max_id}, false)", [])
              _ ->
                raise "Define type of destination database"
            end
          {:error, _} ->
            IO.puts("Transaction failed")
        end
        select_src(select_pid, insert_pid, last_timestamp, limit, table_name, uniques, type)
    end
  end

  def select_src(select_pid, insert_pid, last_timestamp,  limit, table_name, uniques, type) do
    help_map_db = Postgrex.query!(select_pid,
      "SELECT * FROM #{table_name} WHERE updated_at > \'#{last_timestamp}\' ORDER BY updated_at LIMIT #{limit}" |> IO.inspect, [])
    |> get_zip
    updated_at_list = Keyword.get(help_map_db, String.to_atom("updated_at"))
    case length(updated_at_list) do
      0 ->
        select_src(insert_pid, table_name, uniques)
      _ ->
        next_timestamp = Enum.fetch!(updated_at_list,-1)
                         |> NaiveDateTime.to_string
        next_map_db = Postgrex.query!(select_pid,
          "SELECT * FROM #{table_name} WHERE updated_at > \'#{last_timestamp}\' AND updated_at <= \'#{next_timestamp}\' ORDER BY updated_at" |> IO.inspect, [])
        |> get_zip
        insert_query = format_insert_query(next_map_db, table_name, uniques)
        case Postgrex.transaction(insert_pid, fn conn ->
          Postgrex.query!(conn, insert_query, [])
        end) do
          {:ok, _} ->
            case type do
              "postgres" ->
                %Postgrex.Result{rows: [[max_id]]} = Postgrex.query!(insert_pid, "SELECT MAX(#{uniques})FROM #{table_name}" |> IO.inspect, [])
                Postgrex.query!(insert_pid, "ALTER SEQUENCE #{table_name}_#{uniques}_seq RESTART with #{(max_id+1)}", [])
              "cockroach" ->
                %Postgrex.Result{rows: [[max_id]]} = Postgrex.query!(insert_pid, "SELECT MAX(#{uniques})FROM #{table_name}" |> IO.inspect, [])
                Postgrex.query!(insert_pid, "SELECT setval('#{table_name}_seq',#{max_id}, false)", [])
              _ ->
                raise "Define type of destination database"
            end
          {:error, _} ->
            IO.puts("Transaction failed")
        end
        select_src(select_pid, insert_pid, next_timestamp, limit, table_name, uniques, type)
    end
  end

  def select_src(insert_pid, table_name, uniques) do
    IO.puts("Finish migration #{table_name}")
  end

  def format_insert_query(help_map_db, table_name, unique_field) do
    fields = Keyword.keys(help_map_db)
             |> Enum.join(", ")
    values = Keyword.values(help_map_db)
             |> generate_value_string
    "INSERT INTO #{table_name} (#{fields}) VALUES #{values} ON CONFLICT (#{unique_field}) DO NOTHING"
    |> IO.inspect
  end

  defp generate_value_string(values) do
    Enum.zip(values)
    |> Enum.map( fn x ->
      "(" <> (Tuple.to_list(x)
              |> Enum.map(fn x ->
                val_convert(x) end)
              |> Enum.join(", "))
      <> ")" end)
    |> Enum.join(", ")
  end

  def get_zip(%Postgrex.Result{rows: rows, columns: columns,}=result) do
    init_list = Enum.reduce(columns, [], fn column, acc ->
      Keyword.merge(acc, [{String.to_atom(column), []}]) end)
    Enum.reduce(rows, init_list, fn row, acc ->
      Keyword.merge(acc, Enum.zip(columns, row)
                         |> Keyword.new(fn {col, val} ->
                           {String.to_atom(col), val} end), fn _k, v1, v2 ->
                             List.insert_at(v1, -1, v2)
      end)
    end)
    |> IO.inspect
  end

  defp val_convert(value) when is_binary(value) do
    "'" <> to_string(value) <> "'"
  end
  defp val_convert(value) when is_integer(value) do
    to_string(value)
  end

  defp val_convert(value) when is_boolean(value) do
    to_string(value)
  end

  defp val_convert(value) when is_float(value) do
    to_string(value)
  end

  defp val_convert(%Date{}=value) do
    "'" <> Date.to_string(value) <> "'"
  end

  defp val_convert(%NaiveDateTime{}=value) do
    "'" <> NaiveDateTime.to_string(value) <> "'"
  end

  defp val_convert(value) when value==nil do
    "NULL"
  end

  defp val_convert(value) when is_map(value) do
    String.replace(Poison.encode!(value), "\\\\", "")
  end

  defp val_convert(value) when is_list(value) and not is_map(value) do
    app_env = Application.get_env(:cockroach_db, :dest)
    type = Keyword.get(app_env, :type)
    case length(value)  do
      0 ->
        case type do
          "cockroach" ->
            "ARRAY[]"
          "postgres" ->
            "'{}'"
          _ ->
            raise "Define type of destination database"
        end
      _ ->
        case is_map(hd(value)) do
          false ->
            "'{" <> (Enum.map(value, fn val ->
                      val_convert(val) end)
                    |> Enum.join(", "))
            <> "}'"
          true ->
            "'[" <> (Enum.map(value, fn val ->
                      val_convert(val) end)
                    |> Enum.join(", "))
            <> "]'"
        end
    end
  end
end
