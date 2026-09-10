defmodule S3Uploader.FileConsumer do
  @moduledoc """
  A GenStage consumer that processes files.
  """
  use GenStage
  use Private

  alias S3Uploader.FileProducer

  require Logger

  def start_link(config) do
    GenStage.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl true
  def init(args) do
    Logger.info("FileConsumer init: #{inspect(args)}")

    {:consumer, args, subscribe_to: [{FileProducer, max_demand: args[:max_demand] || 1}]}
  end

  @impl true
  def handle_events(events, _from, state) do
    Logger.info("Received events: #{inspect(events)}")

    {:noreply, [], state}
  end
end
