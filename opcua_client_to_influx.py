from opcua import Client
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import time

# =========================
# TIMING CONFIG (ms)
# =========================
READ_INTERVAL_MS = 100              # MUST match server interval
READ_INTERVAL_SEC = READ_INTERVAL_MS / 1000.0

# =========================
# OPC UA CONFIG
# =========================
OPCUA_URL = "opc.tcp://localhost:4840"

# =========================
# INFLUXDB CONFIG
# =========================
INFLUX_URL = "http://localhost:8086"
TOKEN = "YOUR TOKEN HERE"
ORG = "TCS CTO"
BUCKET = "opcua_data"

# =========================
# CONNECT TO INFLUXDB
# =========================
influx_client = InfluxDBClient(
    url=INFLUX_URL,
    token=TOKEN,
    org=ORG
)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# =========================
# CONNECT TO OPC UA SERVER
# =========================
client = Client(OPCUA_URL)
client.connect()
print("Connected to OPC UA Server")

try:
    # =========================
    # DISCOVER NODES SAFELY
    # =========================
    objects = client.get_objects_node()
    object_nodes = objects.get_children()

    mash_tank = None
    for node in object_nodes:
        if node.get_browse_name().Name == "MashTank":
            mash_tank = node
            break

    if mash_tank is None:
        raise Exception("MashTank object not found")

    variables = mash_tank.get_children()

    temperature_node = None
    mash_time_node = None

    for var in variables:
        name = var.get_browse_name().Name
        if name == "temperature":
            temperature_node = var
        elif name == "mashTime":
            mash_time_node = var

    if temperature_node is None or mash_time_node is None:
        raise Exception("Required variables not found")

    print("OPC UA nodes resolved successfully")
    print(f"Client read interval: {READ_INTERVAL_MS} ms")

    # =========================
    # READ → WRITE LOOP
    # =========================
    while True:
        temperature = temperature_node.get_value()
        mash_time = mash_time_node.get_value()

        point = (
            Point("process_data")
            .field("temperature", float(temperature))
            .field("mashTime", int(mash_time))
        )

        write_api.write(bucket=BUCKET, record=point)
        write_api.flush()

        print(
            f"Written → Temp: {temperature} °C | "
            f"MashTime: {mash_time} ms"
        )

        time.sleep(READ_INTERVAL_SEC)

except KeyboardInterrupt:
    print("Stopping client...")

finally:
    client.disconnect()
    influx_client.close()
    print("Disconnected cleanly")
