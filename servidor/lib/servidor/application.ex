defmodule Servidor.Application do
  use Application

  def start(_type, _args) do
    children =
      [
        {Registry, keys: :unique, name: Servidor.StorageRegistry}
      ] ++
      (
        for i <- 1..5, into: [] do
          [
            Supervisor.child_spec(
              {Servidor.Storage, i},
              id: {:storage, i}
            ),
            Supervisor.child_spec(
              {Servidor.Consumer, [name: :"consumer_#{i}", id: i]},
              id: {:consumer, i}
            )
          ]
        end
        |> List.flatten()
      )

    opts = [strategy: :one_for_one, name: Servidor.Supervisor]
    Supervisor.start_link(children, opts)
  end

end
