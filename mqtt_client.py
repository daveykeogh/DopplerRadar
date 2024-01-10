#!/bin/env python

import paho.mqtt.client as mqtt
import json
import time
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import datetime


LOG_FILE = None

class Config(object):
    def __init__(self, config):
        self.username = None
        self.password = None
        self.host = None
        self.port = None
        self.token = None
        self.influx_bucket = None
        self.influx_org = None
        for key, value in config.items():
            setattr(self, key, value)


def load_config(config_file):
    with open(config_file, 'r') as cfg:
        return Config(json.loads(cfg.read()))


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.connected_flag = True
        client.subscribe(userdata.get("topic"))
        print("Connected OK")
    else:
        print("Bad connection. Error code = ", rc)


def on_message(client, userdata, message):
    content = json.loads(message.payload.decode())
    write_api = userdata.get('write_api')
    process_entry(write_api, content)


def speed_translation(speed):
    # Converts from m/s to mph and a direction.
    # Returns a tuple of speed and direction
    direction = "North"
    speed = float(speed)
    if speed < 0:
        # North
        direction = "South"
        # Change the sign as we determined the direction
        speed = speed * -1
    mps_to_mph = 2.23694
    # Based upon the consine of the angle my radar makes with the road I'm monitoring
    angle_compensation = 0.866
    return round((speed * mps_to_mph) / angle_compensation, 1), direction


def process_entry(write_api, entry):
    """
    Processes a speed entry and sends it off to be logged to InfluxDB.
    """
    speed, direction = speed_translation(entry.get('speed'))
    p = influxdb_client.Point("Traffic").tag("Direction", direction).field("speed", speed)
    write_api.write(bucket=config.influx_bucket, org=config.influx_org, record=p)


def main():
    config_file = "config.json"
    config = load_config(config_file)
    influx_client = influxdb_client.InfluxDBClient(
        url="http://{}:8086".format(config.influx_host),
        token=config.token,
        org=config.influx_org
    )
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    while(True):
        try:
            client = mqtt.Client(client_id='InfluxDB', transport='tcp', protocol=mqtt.MQTTv311, clean_session=False)
            client.username_pw_set(config.username, config.password)
            client.user_data_set({"topic": config.topic, 'write_api': write_api})
            client.on_message = on_message
            client.on_connect = on_connect
            client.connect(config.host, port=config.port)
            client.loop_forever()
        except:
            time.sleep(1)


if __name__ == "__main__":
    main()
            