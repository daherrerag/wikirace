defmodule Player.TaskSupervisor do
  @moduledoc """
  Supervises asynchronous tasks shared by all crawler strategies.
  """

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent,
      shutdown: :infinity
    }
  end

  def start_link(opts \\ []) do
    Task.Supervisor.start_link(Keyword.put(opts, :name, __MODULE__))
  end
end
