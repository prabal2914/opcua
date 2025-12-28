from opcua import Server
import time
import random

# =========================
# OPC UA SERVER SETUP
# =========================
server = Server()
server.set_endpoint("opc.tcp://0.0.0.0:4840/")

uri = "http://example.opcua.server"
idx = server.register_namespace(uri)
print("Namespace index:", idx)

objects = server.get_objects_node()
device = objects.add_object(idx, "MashTank")

temperature = device.add_variable(idx, "temperature", 25.0)
mashTime = device.add_variable(idx, "mashTime", 0)

temperature.set_writable()
mashTime.set_writable()

# =========================
# TIMING CONFIG (ms)
# =========================
UPDATE_INTERVAL_MS = 100   # 100 milliseconds
update_interval_sec = UPDATE_INTERVAL_MS / 1000.0

server.start()
print(f"OPC UA Server started (update every {UPDATE_INTERVAL_MS} ms)")

try:
    mash_time_ms = 0

    while True:
        # Simulate temperature change
        new_temp = round(random.uniform(20.0, 100.0), 2)
        temperature.set_value(new_temp)

        # Increment mash time in milliseconds
        mash_time_ms += UPDATE_INTERVAL_MS
        mashTime.set_value(mash_time_ms)

        print(f"Temp: {new_temp} °C | MashTime: {mash_time_ms} ms")

        time.sleep(update_interval_sec)

except KeyboardInterrupt:
    print("Stopping server...")

finally:
    server.stop()
