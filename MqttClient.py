import logging

import paho.mqtt.client as mqtt

from Database import Database


class MqttClient:

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc))

        # Connect to all plant topics
        for plant_id in self.db.getAllPlantID():
            client.subscribe("plant_id/" + str(plant_id))
            self.logging.info("Subscriber to topic of plant_id: " + str(plant_id))


    def on_message(self, client, userdata, msg):
        print(msg.topic + " " + str(msg.payload))

    def __init__(self, config, log: logging, db: Database):
        self.logging = log
        self.db = db
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(config.get("host"), config.get("port"), config.get("keepalive"))
        self.logging.info("MQTT client connected")
        self.client.loop_start()
        self.logging.info("MQTT client listening")