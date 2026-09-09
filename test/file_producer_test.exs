defmodule S3Uploader.FileProducerTest do
  use ExUnit.Case

  alias S3Uploader.FileProducer

  describe "new_files/2" do
    test "returns files newer than the last processed file" do
      files = [
        %{name: "traffic-20260108-2049.log"},
        %{name: "traffic-20260108-2050.log"},
        %{name: "traffic-20260108-2051.log"}
      ]

      last_file = "traffic-20260108-2050.log"

      assert FileProducer.new_files(files, last_file) == [%{name: "traffic-20260108-2051.log"}]
    end

    test "nothing newer than last returns empty" do
      files = [
        %{name: "traffic-20260108-2049.log"},
        %{name: "traffic-20260108-2050.log"},
      ]

      last_file = "traffic-20260108-2050.log"

      assert FileProducer.new_files(files, last_file) == []
    end

    test "only older than last returns empty" do
      files = [
        %{name: "traffic-20260108-2049.log"},
      ]

      last_file = "traffic-20260108-2050.log"

      assert FileProducer.new_files(files, last_file) == []
    end

    test "empty input returns empty list" do
      files = []
      last_file = "traffic-20260108-2050.log"

      assert FileProducer.new_files(files, last_file) == []
    end

    test "only last returns empty" do
      files = [
        %{name: "traffic-20260108-2050.log"}
      ]

      last_file = "traffic-20260108-2050.log"

      assert FileProducer.new_files(files, last_file) == []
    end

    test "missing last returns all" do
      files = [
        %{name: "traffic-20260108-2051.log"},
        %{name: "traffic-20260108-2052.log"}
      ]

      last_file = "traffic-20260108-2050.log"

      assert FileProducer.new_files(files, last_file) == files
    end
  end
end
