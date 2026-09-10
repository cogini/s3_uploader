defmodule S3Uploader.FileProducer do
  @moduledoc """
  A GenStage producer that reads files from a directory.
  """
  use GenStage
  use Private

  require Logger

  def start_link(config) do
    GenStage.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl true
  def init(args) do
    Logger.info("FileProducer init: #{inspect(args)}")

    archive_dir = args[:archive_dir] || Path.join(args[:in_dir], "archive")
    :ok = File.mkdir_p!(archive_dir)

    config = %{
      # Source directory for files
      in_dir: args[:in_dir],

      # Directory to save files after they have been processed
      archive_dir: archive_dir,

      # Ignore files newer than this number of seconds.
      # Avoids processing files that are currently being written.
      min_age: args[:min_age] || 60,

      # Regex matching files to process
      # Files that do not match this pattern will be ignored
      # file_pattern: Regex.compile!(args[:file_pattern] || ".*\\.log$"),
      file_pattern: Regex.compile!(args[:file_pattern] || ".*$"),

      # Files to queue in advance of demand
      readahead_count: args[:readahead_count] || 10,

      # Regex to extract datetime from filename
      datetime_pattern:
        Regex.compile!(
          args[:datetime_pattern] || "\.*-(?<year>\\d{4})(?<month>\\d{2})(?<day>\\d{2}).*"
        ),
    }

    state = %{
      config: config,
      last_file: nil,
      demand: 0,
      queue: :queue.new()
    }

    Logger.debug("state: #{inspect(state)}")

    {:producer, state}
  end

  @impl true
  def handle_demand(incoming_demand, state) do
    %{config: config, demand: demand, queue: queue} = state

    Logger.info("incoming_demand: #{incoming_demand}")

    queue_len = :queue.len(queue)
    new_demand = incoming_demand + demand
    read_count = max(new_demand - queue_len, config.readahead_count)
    {new_queue, new_state} = add_files_to_queue(queue, read_count, state)

    {events, remaining_queue, remaining_demand} = dispatch_events(new_queue, :queue.len(new_queue), new_demand)
    Logger.debug("Dispatching events: #{inspect(events)}")
    Logger.debug("remaining_demand: #{remaining_demand}, remaining_queue: #{inspect(remaining_queue)}")

    {:noreply, events, %{new_state | queue: remaining_queue, demand: remaining_demand}}
  end

  private do
    @spec add_files_to_queue(:queue.queue(), non_neg_integer(), map()) :: {:queue.queue(), map()}
    defp add_files_to_queue(queue, desired_count, state) do
      %{config: config, last_file: last_file} = state

      case read_files(config) do
        {:ok, files} ->
          Logger.debug("Files: #{inspect(files)}")

          # Get files that are newer than the last proccessed file, if any
          new_files = new_files(files, last_file)

          Logger.debug("New files: #{inspect(new_files)}")

          new_files = Enum.take(new_files, desired_count)

          if Enum.empty?(new_files) do
            {queue, state}
          else
            last_file = List.last(new_files).name
            Logger.debug("New last_file: #{inspect(last_file)}")

            queue = Enum.reduce(new_files, queue, &:queue.in/2)
            {queue, %{state | last_file: last_file}}
          end

        {:error, reason} ->
          Logger.error("Error reading from #{config.in_dir}: #{inspect(reason)}")
          {queue, state}
      end
    end

    # Try to fulfil demand from queue
    @spec dispatch_events(:queue.queue(), non_neg_integer(), non_neg_integer()) :: {events :: list(), remaining_queue :: :queue.queue(), remaining_demand :: non_neg_integer()}
    defp dispatch_events(queue, queue_len, demand)
    defp dispatch_events(queue, 0, demand) do
      # queue is empty
      {[], queue, demand}
    end
    defp dispatch_events(queue, queue_len, demand) when queue_len >= demand do
      # queue has enough to satisfy demand
      {events_queue, remaining_queue} = :queue.split(demand, queue)
      {:queue.to_list(events_queue), remaining_queue, 0}
    end
    defp dispatch_events(queue, queue_len, demand) when queue_len < demand do
      # queue does not have enough events to satisfy demand
      {events_queue, remaining_queue} = :queue.split(demand, queue)
      {:queue.to_list(events_queue), remaining_queue, demand - queue_len}
    end

    # Read files from the input directory, filtering by name and age
    @spec read_files(map()) :: {:ok, list(map())} | {:error, File.posix() | :badarg | {:no_translation, binary()}}
    defp read_files(config) do
      %{
        datetime_pattern: datetime_pattern,
        file_pattern: file_pattern,
        in_dir: in_dir,
        min_age: min_age,
      } = config
      
      with {:ok, all_files} <- File.ls(in_dir) do
          Logger.debug("Files in #{in_dir}: #{inspect(all_files)}")

          now = :calendar.datetime_to_gregorian_seconds(:calendar.universal_time())

          files =
            all_files
            |> Enum.filter(&Regex.match?(file_pattern, &1))
            |> Enum.sort()
            |> Enum.map(fn name -> %{name: name, path: Path.join(in_dir, name)} end)
            |> Enum.flat_map(&stat_file/1)
            |> Enum.filter(&by_age(&1, now, min_age))
            |> Enum.map(&get_datetime_from_filename(&1, datetime_pattern))

        {:ok, files}
      end
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

    # Filter function to skip new files
    @spec by_age(map(), integer(), integer()) :: boolean()
    defp by_age(%{path: path, stat: stat}, now, min_age) do
      if age(stat.mtime, now) > min_age do
        true
      else
        Logger.debug("Skipping new file #{path}")
        false
      end
    end

    # Get age in seconds
    defp age(datetime, now) do
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

    # Get files that are newer than the last proccessed file
    # This compares files by name, assuming that they are sorted by date
    @spec new_files(list(map()), binary() | nil) :: list(map())
    defp new_files(files, nil), do: files
    defp new_files(files, last_file) do
      {_old, new} = Enum.split_while(files, fn file -> file.name <= last_file end)
      new
    end
  end
end
