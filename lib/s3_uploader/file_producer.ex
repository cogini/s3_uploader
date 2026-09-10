defmodule S3Uploader.FileProducer do
  @moduledoc """
  A Broadway producer that reads files from a directory.
  """
  use GenStage

  @behaviour Broadway.Producer
  # @behaviour Broadway.Acknowledger

  use Private

  require Logger

  @doc false
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(args) do
    Logger.info("#{__MODULE__} init: #{inspect(args)}")

    config = %{
      # Source directory for files
      in_dir: args[:in_dir],

      # Directory to save files after they have been processed
      archive_dir: args[:archive_dir],

      # Ignore files newer than this number of seconds.
      # Avoids processing files that are currently being written.
      min_age: args[:min_age] || 0,

      # Regex matching files to process
      # Files that do not match this pattern will be ignored
      # file_pattern: Regex.compile!(args[:file_pattern] || ".*\\.log$"),
      file_pattern: Regex.compile!(args[:file_pattern] || ".*$"),

      # Files to queue in advance of demand
      prefetch_count: args[:prefetch_count] || 10,

      # Regex to extract datetime from filename
      datetime_pattern:
        Regex.compile!(
          args[:datetime_pattern] || "\.*-(?<year>\\d{4})(?<month>\\d{2})(?<day>\\d{2}).*"
        ),
    }

    fetch_interval = args[:fetch_interval] || 10_000

    state = %{
      config: config,

      # Unfulfilled demand from consumers
      demand: 0,

      # Prefetch queue to avoid reading dir on every demand
      queue: :queue.new(),

      # Last file read from the input directory
      last_file: nil,

      # How often to check for new files in milliseconds
      fetch_interval: fetch_interval,
    }

    Logger.debug("state: #{inspect(state)}")

    Process.send(self(), :fetch, [])
    {:producer, state}
  end

  @impl true
  def handle_demand(incoming_demand, state) do
    # Fulfil demand from queue
    %{demand: demand, queue: queue} = state

    queue_len = :queue.len(queue)
    Logger.info("incoming_demand: #{incoming_demand}, demand: #{demand}, queue_len: #{queue_len}")

    new_demand = incoming_demand + demand

    {events, remaining_queue, remaining_demand} = dispatch_events(queue, queue_len, new_demand)
    Logger.debug("events: #{inspect(events)}, remaining_queue: #{inspect(remaining_queue)}, remaining_demand: #{remaining_demand}")

    {:noreply, events, %{state | queue: remaining_queue, demand: remaining_demand}}
  end

  # Fetch new files from the input directory and add them to the queue
  @impl true
  def handle_info(:fetch, state) do
    Logger.info("handle_info(:fetch) state: #{inspect(state)}")

    %{config: config, queue: queue, demand: demand} = state

    maybe_garbage_collect()

    queue_len = :queue.len(queue)
    desired_count = config.prefetch_count - queue_len

    {new_queue, new_state} = add_files_to_queue(queue, desired_count, state)

    {events, remaining_queue, remaining_demand} = dispatch_events(new_queue, :queue.len(new_queue), demand)

    Process.send_after(self(), :fetch, state.fetch_interval)
    {:noreply, events, %{new_state | queue: remaining_queue, demand: remaining_demand}}
  end

  private do
    # Fulfil demand from queue
    @spec dispatch_events(:queue.queue(), non_neg_integer(), non_neg_integer()) :: {events :: list(), remaining_queue :: :queue.queue(), remaining_demand :: non_neg_integer()}
    defp dispatch_events(queue, queue_len, demand)

    # queue is empty
    defp dispatch_events(queue, 0, demand) do
      {[], queue, demand}
    end

    # queue has enough to satisfy demand
    defp dispatch_events(queue, queue_len, demand) when queue_len >= demand do
      {events_queue, remaining_queue} = :queue.split(demand, queue)
      {:queue.to_list(events_queue), remaining_queue, 0}
    end

    # queue does not have enough events to satisfy demand
    defp dispatch_events(queue, queue_len, demand) when queue_len < demand do
      {events_queue, remaining_queue} = :queue.split(demand, queue)
      {:queue.to_list(events_queue), remaining_queue, demand - queue_len}
    end

    @spec add_files_to_queue(:queue.queue(), non_neg_integer(), map()) :: {:queue.queue(), map()}
    defp add_files_to_queue(queue, desired_count, state) do
      %{config: config, last_file: last_file} = state

      case read_files(config) do
        {:ok, all_files} ->
          now = :calendar.datetime_to_gregorian_seconds(:calendar.universal_time())

          new_files =
            all_files
            # Get files that are newer than the last proccessed file, if any
            |> new_files(last_file)
            # Restrict the number of files that we have to stat
            |> Enum.take(desired_count)
            # Stat file and filter out directories and other non-regular files
            |> Enum.flat_map(&stat_file/1)
            # Skip files that are newer than the minimum age
            |> Enum.filter(&by_age(&1, now, config.min_age))
            # |> Enum.map(&get_datetime_from_filename(&1, datetime_pattern))

          Logger.debug("new_files: #{inspect(new_files)}")

          if Enum.empty?(new_files) do
            Logger.debug("No new files found in #{config.in_dir}")
            {queue, state}
          else
            last_file = List.last(new_files).name
            new_queue = Enum.reduce(new_files, queue, &:queue.in/2)
            # Logger.debug("new queue len: #{:queue.len(new_queue)}")
            Logger.debug("Added new files to queue: #{length(new_files)}, last_file: #{last_file}")
            {new_queue, %{state | last_file: last_file}}
          end

        {:error, reason} ->
          Logger.error("Error reading files from #{config.in_dir}: #{inspect(reason)}")
            {queue, state}
      end
    end

    # Read files from the input directory, filtering by name and age
    @spec read_files(map()) :: {:ok, list(map())} | {:error, File.posix() | :badarg | {:no_translation, binary()}}
    defp read_files(config) do
      %{file_pattern: file_pattern, in_dir: in_dir} = config
      
      with {:ok, all_files} <- File.ls(in_dir) do
          Logger.debug("all_files in #{in_dir}: #{inspect(all_files)}")

          files =
            all_files
            |> match_names(file_pattern)
            |> Enum.sort()
            |> Enum.map(fn name -> %{file_name: name, path: Path.join(in_dir, name)} end)

        {:ok, files}
      end
    end

    # Select names that match Regex pattern, if any
    defp match_names(names, nil), do: names
    defp match_names(names, file_pattern) do
      Enum.filter(names, fn name -> Regex.match?(file_pattern, name) end)
    end

    # Get files that are newer than the last proccessed file, if any
    # This compares files by name, assuming that they are sorted by date
    @spec new_files(list(map()), binary() | nil) :: list(map())
    defp new_files(events, nil), do: events
    defp new_files(events, last_file) do
      {_old, new} = Enum.split_while(events, fn event -> event.name <= last_file end)
      new
    end

    # Stat file and and filter out directories and other non-regular files
    @spec stat_file(map()) :: list(map())
    defp stat_file(%{path: path} = rec) do
      case File.stat!(path, time: :universal) do
        %{type: :regular} = stat ->
          [Map.put(rec, :stat, stat)]

        %{type: :directory} ->
          # Logger.debug("Skipping #{type} #{path}")
          []

        %{type: type} ->
          Logger.debug("Skipping #{type} #{path}")
          []
      end
    end

    # Filter to skip new files
    @spec by_age(map(), integer(), integer()) :: boolean()
    defp by_age(%{path: path, stat: stat}, now, min_age) do
      if age_in_seconds(stat.mtime, now) > min_age do
        true
      else
        Logger.debug("Skipping new file #{path}")
        false
      end
    end

    # Get age in seconds
    defp age_in_seconds(datetime, now) do
      now - :calendar.datetime_to_gregorian_seconds(datetime)
    end

    # Extract datetime from filename using Regex pattern
    defp get_datetime_from_filename(%{path: path} = rec, pattern) do
      {:ok, datetime} = filename_to_datetime(path, pattern)
      datetime_path = datetime_to_path(datetime)
      Map.merge(rec, %{datetime: datetime, datetime_path: datetime_path})
    end

    # Get datetime from filename using Regex pattern
    @spec filename_to_datetime(binary(), Regex.t()) :: {:ok, DateTime.t()}
    defp filename_to_datetime(filename, pattern) do
      named_captures = Regex.named_captures(pattern, filename)

      {:ok, date} =
        Date.new(
          String.to_integer(named_captures["year"]),
          String.to_integer(named_captures["month"]),
          String.to_integer(named_captures["day"])
        )

      {:ok, time} = Time.new(0, 0, 0, 0)
      DateTime.new(date, time, "Etc/UTC")
    end

    # Format datetime as path string "YYYY/MM/DD"
    @spec datetime_to_path(DateTime.t()) :: binary()
    defp datetime_to_path(datetime) do
      year = datetime.year |> Integer.to_string() |> String.pad_leading(4, "0")
      month = datetime.month |> Integer.to_string() |> String.pad_leading(2, "0")
      day = datetime.day |> Integer.to_string() |> String.pad_leading(2, "0")

      Path.join([year, month, day])
    end

  end

  # Manually trigger garbage collection to clear refc binary memory
  @spec maybe_garbage_collect() :: :ok
  defp maybe_garbage_collect do
    case :recon.info(self(), :binary_memory) do
      {:binary_memory, binary} when binary > 50_000_000 ->
        Logger.debug("Forcing garbage collection")
        :erlang.garbage_collect(self())

      _ ->
        :ok
    end
  end
end
