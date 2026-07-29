defmodule S3UploaderTest do
  use ExUnit.Case
  doctest S3Uploader

  test "greets the world" do
    assert S3Uploader.hello() == :world
  end
end
