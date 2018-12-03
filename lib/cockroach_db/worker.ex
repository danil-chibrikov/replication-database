defmodule CockroachDb.Worker do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(_) do
    recursive_loop()
    {:ok, %{}}
  end

  def recursive_loop() do
    select_pid = Process.whereis(:source)
    insert_pid = Process.whereis(:destination)
    app_env = Application.get_all_env(:cockroach_db)
              |> Enum.into(%{})

    limit = get_config_value(app_env, :limit)
    select_name = get_config_value(app_env, :select_table_and_unique)
    type = Keyword.get(get_config_value(app_env, :dest), :type)
    Enum.map(select_name, fn {table_name, uniques} ->
      first(select_pid, insert_pid, Atom.to_string(table_name), limit, uniques, type)
    end)

    Process.sleep(5000)
    recursive_loop()
  end

  def get_config_value(list, atom) do
    Map.get(list, atom)
  end

  def find_max(pid, table_name) do
    %Postgrex.Result{rows: [[max]]} =
      Postgrex.query!(pid, "SELECT MAX(updated_at) FROM #{table_name}", [])
    max
  end

  def first(select_pid, insert_pid, table_name, limit, uniques, type) do
    max_src_updated_at = find_max(select_pid, table_name)
    max_dest_updated_at = find_max(insert_pid, table_name)
    case max_dest_updated_at do
      nil ->
        map_db = Postgrex.query!(select_pid,
          "SELECT * FROM #{table_name} WHERE updated_at < '#{max_src_updated_at}' ORDER BY updated_at LIMIT #{limit}" |> IO.inspect, [])
        |> get_zip
        updated_at_list = Keyword.get(map_db, String.to_atom("updated_at"))
        last_timestamp = find_timestamp(updated_at_list)
        select_src(select_pid, insert_pid, limit, table_name, uniques, type, max_src_updated_at, last_timestamp)
      _ ->
        if max_src_updated_at != max_dest_updated_at do
          select_src(select_pid, insert_pid, limit, table_name, uniques, type, max_src_updated_at, max_dest_updated_at)
        end
        IO.puts("Already updated")
    end
  end

  def select_src(select_pid, insert_pid, limit, table_name, uniques, type, max_timestamp, last_timestamp) do
    next_map_db = Postgrex.query!(select_pid,
      "SELECT * FROM #{table_name} WHERE updated_at > '#{last_timestamp}' AND updated_at <= '#{max_timestamp}' ORDER BY updated_at" |> IO.inspect, [])
    |> get_zip
    updated_at_list = Keyword.get(next_map_db, String.to_atom("updated_at"))
    case length(updated_at_list) do
      0 ->
        select_src(table_name)
      _ ->
        insert_query = format_insert_query(next_map_db, table_name, uniques)
        check_type_db(insert_pid, insert_query, type, table_name, uniques)
        select_src(select_pid, insert_pid, limit, table_name, uniques, type, max_timestamp, find_timestamp(updated_at_list))
    end
  end

  def select_src(table_name) do
    IO.puts("Finish migration #{table_name}")
  end

  def check_type_db(insert_pid, insert_query, type, table_name, uniques) do
    case Postgrex.transaction(insert_pid, fn conn ->
      Postgrex.query!(conn, insert_query, []) end) do
      {:ok, _} ->
        max_id = case_query(insert_pid, "SELECT MAX(#{uniques}) FROM #{table_name}")
        case type do
          "postgres" ->
            Postgrex.query!(insert_pid, "ALTER SEQUENCE #{table_name}_#{uniques}_seq RESTART with #{(max_id+1)}", [])
          "cockroach" ->
            Postgrex.query!(insert_pid, "SELECT setval('#{table_name}_seq',#{max_id}, false)", [])
          _ ->
            raise "Define type of destination database"
        end
      {:error, _} ->
        IO.puts("Transaction failed")
    end
  end

  def case_query(insert_pid, query_db) do
    %Postgrex.Result{rows: [[max_id]]} = Postgrex.query!(insert_pid, query_db, [])
    max_id
  end

  def find_timestamp(updated_at_list) do
    Enum.fetch!(updated_at_list, -1) |> NaiveDateTime.to_string
  end

  def format_insert_query(help_map_db, table_name, unique_field) do
    fields = Keyword.keys(help_map_db)
             |> Enum.join(", ")
    values = Keyword.values(help_map_db)
             |> generate_value_string
    "INSERT INTO #{table_name} (#{fields}) VALUES #{values} ON CONFLICT (#{unique_field}) DO NOTHING" |> IO.inspect
  end

  defp map_create(value) do
    Enum.map(value, fn val ->
      val_convert(val) end)
    |> Enum.join(", ")
  end

  defp generate_value_string(values) do
    Enum.zip(values)
    |> Enum.map(fn x ->
      "(" <> map_create(Tuple.to_list(x)) <> ")" end)
    |> Enum.join(", ")
  end

  def get_zip(%Postgrex.Result{rows: rows, columns: columns}) do
    init_list = Enum.reduce(columns, [], fn column, acc ->
      Keyword.merge(acc, [{String.to_atom(column), []}]) end)
    Enum.reduce(rows, init_list, fn row, acc ->
      Keyword.merge(acc, Enum.zip(columns, row)
                         |> Keyword.new(fn {col, val} ->
                           {String.to_atom(col), val} end), fn _k, v1, v2 ->
                             List.insert_at(v1, -1, v2)
      end)
    end) |> IO.inspect
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
          "cockroach" -> "ARRAY[]"
          "postgres" -> "'{}'"
          _ -> raise "Define type of destination database"
        end
      _ ->
        case is_map(hd(value)) do
          false -> "'{" <> map_create(value) <> "}'"
          true -> "'[" <> map_create(value) <> "]'"
        end
    end
  end
end
