import Config

config :ex_aws,
  hackney_opts: [
    pool: :ex_aws_pool,
    # Max time to wait for a pool connection in ms
    checkout_timeout: 8_000,
    # Time to wait for a socket connection to be esablished in ms. Default 8000.
    connect_timeout: 10_000,
    # HTTP/2 body send timeout or flow control write timeout (default 30000 ms or pool-dependent)
    # send_timeout: 30_000,
    # Max time to wait for a response in ms after request is sent. Default 5000.
    recv_timeout: 15_000
  ],
  access_key_id: [{:system, "AWS_ACCESS_KEY_ID"}, :instance_role],
  secret_access_key: [{:system, "AWS_SECRET_ACCESS_KEY"}, :instance_role],
  # security_token: [{:system, "AWS_SESSION_TOKEN"}, :instance_role],
  region: {:system, "AWS_REGION"}

# config :hackney,
#   pool: [max_connections: 50]

config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:pid, :module, :function, :file, :line]

config :logger,
  level: :info

import_config "#{config_env()}.exs"
