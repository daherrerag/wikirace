defmodule Player.CrawlerSupervisor do
  # Ejecutar porcesos de manera dinamica, es decir el proceso se muere y lo puedo iniciar /finalizar de manera dinamica
  use DynamicSupervisor

  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(
      __MODULE__,
      :ok,
      Keyword.put(opts, :name, __MODULE__)
    )
  end

  @impl true
  def init(:ok) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
