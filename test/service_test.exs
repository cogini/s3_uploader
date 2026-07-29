defmodule S3Uploader.ServiceTest do
  use ExUnit.Case, async: true

  alias S3Uploader.Service

  describe "filename_to_datetime/2" do
    test "extracts datetime from filename" do
      filename = "traffic-20260108-2049.log"

      file_pattern = ~r/traffic-(?<year>\d{4})(?<month>\d{2})(?<day>\d{2})/
      {:ok, datetime} = Service.filename_to_datetime(filename, file_pattern)
      assert DateTime.diff(datetime, DateTime.new!(~D[2026-01-08], ~T[00:00:00], "Etc/UTC")) == 0
    end
  end

  describe "datetime_to_path/1" do
    test "generates path from datetime" do
      datetime = DateTime.new!(~D[2026-01-08], ~T[00:00:00], "Etc/UTC")
      path = Service.datetime_to_path(datetime)
      assert path == "2026/01/08"
    end
  end
end
