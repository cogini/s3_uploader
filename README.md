![test workflow](https://github.com/cogini/kubernetes_health_check/actions/workflows/test.yml/badge.svg)
[![Module Version](https://img.shields.io/hexpm/v/kubernetes_health_check.svg)](https://hex.pm/packages/kubernetes_health_check)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/kubernetes_health_check)
[![Total Download](https://img.shields.io/hexpm/dt/kubernetes_health_check.svg)](https://hex.pm/packages/kubernetes_health_check)
[![License](https://img.shields.io/hexpm/l/kubernetes_health_check.svg)](https://github.com/cogini/kubernetes_health_check/blob/master/LICENSE.md)
[![Last Updated](https://img.shields.io/github/last-commit/cogini/kubernetes_health_check/main)](https://github.com/cogini/kubernetes_health_check/commits/main)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](CODE_OF_CONDUCT.md)

# s3_uploader

A service that uploads files to AWS S3.

It periodically looks for new files in a specificed directory, uploads
them to S3, then moves them to an archive directory structure named by date.

It parses the date from the file name to determine where to archive them.
Files look like `traffic-20260108-2049.log`.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `s3_uploader` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:s3_uploader, "~> 0.7.0"}
  ]
end
```

Configure the uploader in `config.exs`:

```elixir
config :foo, :s3_uploader,
  sources: [
    traffic: [
      max_concurrency: 10,
      batch_size: 50,
      in_dir: "/var/log/foo/traffic/archive",
      archive_dir: "/var/log/foo/traffic/archive",
      file_pattern: "traffic-\\d{8}-\\d{4}\\.log$",
      datetime_pattern: "traffic-(?<year>\\d{4})(?<month>\\d{2})(?<day>\\d{2})",
      bucket: "foo-prod-traffic-logs",
      bucket_prefix: "traffic",
      bucket_region: "eu-central-1"
    ]
  ]
```

Add it to your application supervision tree:

```elixir
  @app :foo

  @impl true
  def init(args) do
    children =
      List.flatten([
        s3_uploader_spec(args)
      ])

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 1000, max_seconds: 300)
  end

  defp s3_uploader_spec(args) do
    s3_uploder_config = Application.get_env(@app, :s3_uploader)

    if s3_uploder_config do
      sources = s3_uploder_config[:sources] || []

      :ok = :hackney_pool.start_pool(:ex_aws_pool, timeout: 15_000, max_connections: 40)

      # [{Hackney.Pool, name: :ex_aws_pool, max_connections: 40, timeout: 15_000}] ++
      for {name, config} <- sources do
        id = String.to_atom("s3_uploader_#{name}")
        config = Keyword.put(config, :hostname, args[:hostname])
        Logger.info("Starting S3 Uploader #{name}: #{inspect(config)}")
        %{id: id, start: {S3Uploader, :start_link, [config]}}
      end
    else
      []
    end
  end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/s3_uploader>.
