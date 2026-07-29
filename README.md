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

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/s3_uploader>.

