defmodule S3Uploader.Service do
  @moduledoc """
  GenServer which uploads log files to an S3 bucket.

  It periodically looks for new files in a specificed directory, uploads
  them to S3, then moves them to an archive directory structure named by date.

  It parses the date from the file name to determine where to archive them.
  Files look like traffic-20260108-2049.log
  """
  use GenServer

  require Logger

  @doc "Start the server"
  def start_link do
    GenServer.start_link(__MODULE__, [], [])
  end

  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  # GenServer callbacks

  def init(args) do
    Logger.info("init: #{inspect(args)}")

    hostname = args[:hostname]
    bucket_prefix = Path.join(hostname, args[:bucket_prefix])

    check_delay = args[:check_delay] || 60_000
    archive_dir = args[:archive_dir]
    :ok = File.mkdir_p!(archive_dir)

    state = %{
      # Source directory for files
      in_dir: args[:in_dir],

      # Directory to save files after they have been uploaded
      archive_dir: archive_dir,

      # Time to wait if there are no files in the source dir, in ms
      check_delay: check_delay,

      # Ignore files newer than this number of seconds.
      # Avoids processing files that are currently being written.
      min_age: args[:min_age] || 60,

      # Maximum number of uploads to run concurrently
      max_concurrency: args[:max_concurrency] || System.schedulers_online(),

      # Number of files to process in a single batch
      # Used to limit logging output, providing statistics per batch
      batch_size: args[:batch_size] || System.schedulers_online(),

      # Regex matching files to upload
      file_pattern: Regex.compile!(args[:file_pattern] || ".*\\.log$"),

      # Regex to extract datetime from filename
      datetime_pattern:
        Regex.compile!(
          args[:datetime_pattern] || "\.*-(?<year>\\d{4})(?<month>\\d{2})(?<day>\\d{2}).*"
        ),

      # Name of S3 bucket
      bucket: args[:bucket],

      # File prefix for uploads, e.g., "traffic"
      bucket_prefix: bucket_prefix,

      # AWS region where S3 bucket is set up
      bucket_region: args[:bucket_region] || "us-east-1",

      # Max time for an upload task to run before it is considered failed, in ms
      timeout: args[:timeout] || 60_000
    }

    Logger.debug("state: #{inspect(state)}")

    Process.send_after(self(), :process, check_delay)
    {:ok, state}
  end

  def handle_info(:process, state) do
    Logger.debug("handle_info: :process")
    process_files(state)

    Process.send_after(self(), :process, state.check_delay)
    {:noreply, state}
  end

  def handle_info(message, state) do
    Logger.info(fn -> "Unexpected message: #{inspect(message)}" end)
    {:noreply, state, state.check_delay}
  end

  defp process_files(state) do
    Logger.debug("process_files function")

    %{
      archive_dir: archive_dir,
      batch_size: batch_size,
      datetime_pattern: datetime_pattern,
      file_pattern: file_pattern,
      in_dir: in_dir,
      max_concurrency: max_concurrency,
      min_age: min_age,
      timeout: timeout
    } = state

    {:ok, all_files} = File.ls(in_dir)
    Logger.debug("Files in #{in_dir}: #{inspect(all_files)}")

    now = :calendar.datetime_to_gregorian_seconds(:calendar.universal_time())

    batches =
      all_files
      |> Enum.filter(&by_name(&1, file_pattern))
      |> Enum.sort()
      |> Enum.map(fn name -> %{name: name, path: Path.join(in_dir, name)} end)
      |> Enum.map(&get_datetime_from_filename(&1, datetime_pattern))
      |> Enum.flat_map(&stat_file/1)
      |> Enum.filter(&by_age(&1, now, min_age))
      |> Enum.chunk_every(batch_size)

    for batch <- batches do
      Logger.debug("Processing batch of #{length(batch)} files")
      # Logger.debug("Batch files: #{inspect(batch)}")

      info = prepare_batch(batch)
      Logger.debug("Batch info: #{inspect(info)}")
      make_dirs(info.datetime_paths, archive_dir)

      start_time = :erlang.system_time(:millisecond)

      stream =
        Task.async_stream(
          batch,
          &process_file(&1, state),
          max_concurrency: max_concurrency,
          timeout: timeout
        )

      Stream.run(stream)

      dur_ms = :erlang.system_time(:millisecond) - start_time

      %{size_mb: size_mb, dur: dur, rate: rate} = batch_stats(info, dur_ms)
      lag = DateTime.diff(DateTime.utc_now(), info.last.datetime, :second)

      Logger.info(
        "Uploaded #{info.last.name} #{info.count} files in #{dur}s #{size_mb} MB (#{rate} MB/s) lag #{lag}s"
      )
    end
  end

  defp batch_stats(info, 0), do: batch_stats(info, 1)

  defp batch_stats(info, dur_ms) do
    size_mb = Float.round(info.size / (1024.0 * 1024.0), 2)
    dur = Float.round(dur_ms / 1000.0, 3)
    rate = Float.round(size_mb / dur, 2)
    %{size_mb: size_mb, dur: dur, rate: rate}
  end

  # Create archive directories
  def make_dirs(paths, archive_dir) do
    paths
    |> Enum.uniq()
    |> Enum.map(fn path -> :ok = File.mkdir_p(Path.join(archive_dir, path)) end)
  end

  @spec process_file(map(), map()) :: :ok
  def process_file(rec, state) do
    %{name: name, path: path, datetime_path: datetime_path} = rec

    dest_path = Path.join([state.archive_dir, datetime_path, name])
    s3_path = Path.join(state.bucket_prefix, dest_path)

    Logger.debug("Uploading file #{path} to s3://#{state.bucket}/#{s3_path}")

    path
    |> ExAws.S3.Upload.stream_file()
    |> ExAws.S3.upload(state.bucket, s3_path,
      timeout: state.timeout,
      bucket_region: state.bucket_region
    )
    |> ExAws.request!()

    Logger.debug("Moving file #{path} to archive #{dest_path}")
    :ok = File.rename(path, dest_path)
  end

  def prepare_batch(batch) do
    Enum.reduce(batch, %{size: 0, count: 0, datetime_paths: [], last: nil}, fn %{
                                                                                 stat: stat,
                                                                                 datetime_path:
                                                                                   path
                                                                               } = rec,
                                                                               acc ->
      %{
        size: acc.size + stat.size,
        count: acc.count + 1,
        datetime_paths: [path | acc.datetime_paths],
        last: rec
      }
    end)
  end

  # Filter function to test if filename matches Regex pattern
  @spec by_name(binary(), Regex.t()) :: boolean()
  def by_name(filename, pattern) do
    Regex.match?(pattern, filename)
  end

  @spec stat_file(map()) :: list(map())
  def stat_file(%{path: path} = rec) do
    case File.stat(path, time: :universal) do
      {:ok, %{type: :regular} = stat} ->
        [Map.put(rec, :stat, stat)]

      {:ok, %{type: :directory}} ->
        # Logger.debug("Skipping #{type} #{path}")
        []

      {:ok, %{type: type}} ->
        Logger.debug("Skipping #{type} #{path}")
        []

      {:error, reason} ->
        Logger.error("Could not stat file #{path}: #{reason}")
        []
    end
  end

  # Filter function to skip new files
  @spec by_age(map(), integer(), integer()) :: boolean()
  def by_age(%{path: path, stat: stat}, now, min_age) do
    if age(stat.mtime, now) > min_age do
      true
    else
      Logger.debug("Skipping new file #{path}")
      false
    end
  end

  @doc "Get file age in seconds"
  def age(datetime, now) do
    now - :calendar.datetime_to_gregorian_seconds(datetime)
  end

  # Extract datetime from filename using Regex pattern
  def get_datetime_from_filename(%{path: path} = rec, pattern) do
    {:ok, datetime} = filename_to_datetime(path, pattern)
    datetime_path = datetime_to_path(datetime)
    Map.merge(rec, %{datetime: datetime, datetime_path: datetime_path})
  end

  # Get datetime from filename using Regex pattern
  @spec filename_to_datetime(binary(), Regex.t()) :: {:ok, DateTime.t()}
  def filename_to_datetime(filename, pattern) do
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

  @spec datetime_to_path(DateTime.t()) :: binary()
  def datetime_to_path(datetime) do
    year = datetime.year |> Integer.to_string() |> String.pad_leading(4, "0")
    month = datetime.month |> Integer.to_string() |> String.pad_leading(2, "0")
    day = datetime.day |> Integer.to_string() |> String.pad_leading(2, "0")

    Path.join([year, month, day])
  end
end
