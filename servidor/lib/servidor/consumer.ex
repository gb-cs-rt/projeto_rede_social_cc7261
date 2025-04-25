defmodule Servidor.Consumer do
    use GenServer
    require Logger
    alias AMQP.{Connection, Channel, Basic, Queue}

    def start_link(opts) do
      name = Keyword.fetch!(opts, :name)
      GenServer.start_link(__MODULE__, opts, name: name)
    end

    def init(opts) do
        id = opts[:id]
        storage_pid = opts[:storage_pid]

        Logger.info("📡 [#{id}] Conectando ao RabbitMQ...")

        case Connection.open("amqp://guest:guest@localhost") do
          {:ok, conn} ->
            {:ok, chan} = Channel.open(conn)
            Queue.declare(chan, "post_queue", durable: true)
            Basic.consume(chan, "post_queue")
            Logger.info("🎧 [#{id}] Escutando fila 'post_queue'...")

            {:ok, %{conn: conn, chan: chan, consumer_id: id}}

          {:error, reason} ->
            Logger.error("❌ [#{id}] Falha na conexão: #{inspect(reason)}")
            {:stop, reason}
        end
    end

    def handle_info({:basic_consume_ok, %{consumer_tag: consumer_tag}}, state) do
      Logger.info("✅ Consumidor registrado com tag: #{consumer_tag}")
      {:noreply, state}
    end

    def handle_info({:basic_cancel, %{consumer_tag: consumer_tag}}, state) do
      Logger.warning("⚠️ Consumidor cancelado: #{consumer_tag}")
      {:stop, :normal, state}
    end

    def handle_info({:basic_cancel_ok, %{consumer_tag: consumer_tag}}, state) do
      Logger.info("ℹ️ Cancelamento confirmado: #{consumer_tag}")
      {:noreply, state}
    end

    def handle_info({:basic_deliver, payload, _meta}, %{consumer_id: id} = state) do
        case Jason.decode(payload) do
          {:ok, %{"user" => user, "content" => content}} ->
            Logger.info("[#{id}] 📥 #{user}: #{content}")

            case Registry.lookup(Servidor.StorageRegistry, "storage_#{id}") do
              [{pid, _}] -> GenServer.cast(pid, {:save_post, user, content})
              [] -> Logger.error("❌ Storage não encontrado para consumidor #{id}")
            end

          {:error, _} ->
            Logger.warning("[#{id}] ⚠️ JSON inválido: #{payload}")
        end

        {:noreply, state}
    end

    def handle_info(msg, state) do
      Logger.debug("🔄 Mensagem não tratada: #{inspect(msg)}")
      {:noreply, state}
    end
  end
