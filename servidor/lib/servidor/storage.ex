defmodule Servidor.Storage do
  use GenServer
  require Logger

  def start_link(consumer_id) do
    name = {:via, Registry, {Servidor.StorageRegistry, "storage_#{consumer_id}"}}
    GenServer.start_link(__MODULE__, consumer_id, name: name)
  end

  def init(consumer_id) do
    table = :"posts_#{consumer_id}"
    :ets.new(table, [:named_table, :public, :set])
    load_from_disk(table, consumer_id)
    Process.send_after(self(), :save, 10_000)
    {:ok, %{id: consumer_id, table: table}}
  end

  def list_posts(consumer_id) do
    :ets.tab2list(:"posts_#{consumer_id}")
  end

  def handle_cast({:save_post, user, content}, state) do
    id = System.system_time(:millisecond)
    :ets.insert(state.table, {id, user, content})
    {:noreply, state}
  end

  def handle_info(:save, state) do
    posts =
      :ets.tab2list(state.table)
      |> Enum.map(fn {id, user, content} ->
        %{"id" => id, "user" => user, "content" => content}
      end)

    File.write!(filename_for(state.id), Jason.encode!(posts))
    Process.send_after(self(), :save, 10_000)

    {:noreply, state}
  end

  defp load_from_disk(table, id) do
    filename = filename_for(id)

    case File.read(filename) do
      {:ok, content} ->
        case Jason.decode(content) do
          {:ok, posts} ->
            Enum.each(posts, fn {id, user, content} ->
              :ets.insert(table, {id, user, content})
            end)

          _ ->
            Logger.warning("⚠️ Arquivo corrompido: #{filename}")
        end

      _ -> :ok
    end
  end

  defp filename_for(id), do: "posts_consumer_#{id}.json"
end
