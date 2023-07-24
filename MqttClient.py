import logging

import paho.mqtt.client as mqtt
from socket import gaierror
from MessageHandler import MessageHandler


class MqttClient:

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc))

        # Connect to all plant topics
        for plant_id in self.go.getAllPlantID():
            client.subscribe("plant_id/" + str(plant_id))
            print("Subscribed to topic : plant_id/" + str(plant_id))
            self.logging.info("Subscribed to topic : plant_id/" + str(plant_id))

    def on_message(self, client, userdata, msg):
        m = MessageHandler(self.logging, msg.payload.decode('utf-8'), msg.topic, self.go)
        m.start()

    def __init__(self, config, log: logging, go):
        self.logging = log
        self.config = config
        self.go = go
        self.client = mqtt.Client()

    def start(self):
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        try:
            self.client.connect(self.config.get("host"), self.config.get("port"), self.config.get("keepalive"))
            self.logging.info("MQTT client connected")
            self.client.loop_start()
            self.logging.info("MQTT client listening")
        except ConnectionRefusedError as e:
            self.logging.error("Cannot connect to MQTT host: " + str(self.config.get("host")) + ":" + str(self.config.get("port")))
            print("Cannot connect to MQTT host: " + str(self.config.get("host")) + ":" + str(self.config.get("port")))
            exit(1)
        except gaierror:
            self.logging.error("Host: [" + str(self.config.get("host")) + "] not found")
            print("Host not found")
            exit(1)

    def send_message(self, topic: str, payload: str):
        try:
            self.client.connect(self.config.get("host"), self.config.get("port"), self.config.get("keepalive"))
            self.logging.info("MQTT broker connected")
            self.client.publish(topic, payload)
            self.client.disconnect()
            self.logging.info("Disconnect after message send")
            return True
        except ConnectionRefusedError as e:
            self.logging.warning("Cannot connect to MQTT host: " + str(self.config.get("host")) + ":" + str(self.config.get("port")))
            print("Cannot connect to MQTT host: " + str(self.config.get("host")) + ":" + str(self.config.get("port")) + " - Message probably not sent")
        except gaierror:
            self.logging.error("Host: [" + str(self.config.get("host")) + "] not found")
            print("Host not found - Message probably not sent")
        return False
