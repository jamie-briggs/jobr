import Config

# MQTT bridge configuration.
#
# `Jobr.Mqtt` connects to an MQTT broker and republishes every inbound message
# onto `Jobr.Broker`, so the rest of the application only ever talks to the
# local broker and never sees MQTT specifics.
#
# `protocol_version` accepts 5 (MQTT 5.0) or 4 (MQTT 3.1.1). Both are supported
# by the same client module; see `Jobr.Mqtt` for the behavioural differences.
config :jobr, Jobr.Mqtt,
  host: "localhost",
  port: 1883,
  client_id: "jobr-#{:erlang.node()}",
  protocol_version: 5,
  keepalive: 30,
  # Topics to subscribe to once connected. Wildcards are subscribe-only.
  topics: ["jobr/#"],
  qos: 1,
  # Start the client with the application? Tests start it explicitly instead.
  enabled: config_env() == :prod
