import logging
from threading import Thread


class MessageHandler(Thread):

    def __init__(self, log: logging, message: str, topic: str, go):
        super().__init__()
        self.message_values = 0
        self.plant_id = None
        self.logging = log
        self.topic = topic
        self.message = message
        self.go = go

    def run(self):
        try:
            self.parseTopic()
            self.parseMessage()
        except ValueError as e:
            self.logging.warning("Cannot parse this message [" + str(e) + "]")

    def parseMessage(self):
        print("Parso il messaggio dal topic: " + str(self.topic) + ": " + str(self.message))
        tokens = self.message.split('_')
        if self.validTokens(tokens):
            method = tokens[0]
            if method.lower() == "d":
                self.manageNewDetection(tokens)
            elif method.lower() == "w":
                self.manageWateringAck(tokens)
            else:
                self.logging.warning("Unkown method [" + str(method) + "]")
                raise ValueError("Unkown method")
        else:
            self.logging.warning("Invalid message received [" + str(self.message) + "]")
            raise ValueError("Invalid message received")

    def parseTopic(self):
        try:
            self.plant_id = int(self.topic.replace('plant_id/', ''))
        except ValueError as e:
            self.logging.warning("Invalid topic [" + str(self.topic) + "]")
            raise e

    def validTokens(self, tokens):
        self.message_values = len(tokens)
        return self.message_values > 1

    def manageWateringAck(self, tokens):
        if self.message_values == 2:
            watering_id = int(tokens[1])
            self.go.ack_watering(watering_id)
            self.logging.debug("Confirmed watering - plant_id " + str(self.plant_id))
        else:
            self.logging.warning("Cannot manage this message as a watering ack: [" + str(self.message) + "]")
            raise ValueError("Cannot manage this message as a watering ack")

    def manageNewDetection(self, tokens):
        if self.message_values == 3:
            humidity = int(tokens[1])
            sensor_id = int(tokens[2])
            det_id = self.go.add_detection(self.plant_id, humidity, sensor_id)
            self.logging.debug("Added detection [" + str(det_id) + "] - plant_id " + str(self.plant_id) + " - hum: " + str(humidity) + " - sensor: " + str(sensor_id))
        else:
            self.logging.warning("Cannot manage this message as a detection: [" + str(self.message) + "]")
            raise ValueError("Cannot manage this message as a detection")
