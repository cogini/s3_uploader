import Config

config :ex_aws,
  hackney_options: [
    pool: :ex_aws_pool,
    # Max time to wait for a connection (in ms)
    checkout_timeout: 8_000,
    # Max time to wait for a response (in ms)
    recv_timeout: 15_000
  ],
  access_key_id: [{:system, "AWS_ACCESS_KEY_ID"}, :instance_role],
  secret_access_key: [{:system, "AWS_SECRET_ACCESS_KEY"}, :instance_role],
  region: {:system, "AWS_REGION"}

config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:pid, :module, :function, :line]

config :logger,
  level: :info

import_config "#{config_env()}.exs"
