ExUnit.start()

# Start the in-process MQTT broker once for the whole suite, so tests exercise
# the real wire protocol without needing Mosquitto or EMQX installed.
{:ok, _pid} = Jobr.TestBroker.start_link()
