import logging
from threading import Thread


class MessageHandler(Thread):

    def __init__(self, log: logging, message: str, topic: str, go):
        super().__init__()
        self.message_values = 0
        self.plant_id = None
        self.sensor_id = None
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
                self.managePlantDetection(tokens)
            elif method.lower() == "d2":
                self.manageSensorDetection(tokens)
            elif method.lower() == "w":
                self.manageWateringAck(tokens)
            elif method.lower() == "s":
                self.manageGreeting(tokens)
            else:
                self.logging.warning("Unkown method [" + str(method) + "]")
                raise ValueError("Unkown method")
        else:
            self.logging.warning("Invalid message received [" + str(self.message) + "]")
            raise ValueError("Invalid message received")

    def parseTopic(self):
        try:
            if self.topic.startswith("sensor/"):
                self.sensor_id = int(self.topic.replace('sensor/', ''))
            elif self.topic.startswith("greeting"):
                self.logging.info("Greeting message")
            else:
                self.logging.warning(f"Unexpected topic {self.topic}")
                print(f"Unexpected topic {self.topic}")
        except ValueError as e:
            self.logging.warning("Invalid topic [" + str(self.topic) + "]")
            raise e

    def validTokens(self, tokens):
        self.message_values = len(tokens)
        return self.message_values > 1

    def manageGreeting(self, tokens):
        if self.message_values == 2:
            self.sensor_id = int(tokens[1])
            if self.sensor_id in self.go.getAllSensorID():
                self.logging.info(f"Sensor #{self.sensor_id} restarted")
            else:
                self.logging.info(f"New sensor [#{self.sensor_id}] connected. Start listening on its topic")
                self.go.add_sensor(self.sensor_id)
        else:
            self.logging.warning("Cannot manage this message as a watering ack: [" + str(self.message) + "]")
            raise ValueError("Cannot manage this message as a watering ack")

    def manageSensorDetection(self, tokens):
        if self.message_values == 3:
            humidity = int(tokens[1])
            plant_num = int(tokens[2])
            self.plant_id = self.go.getPlantID(self.sensor_id, plant_num)
            det_id = self.go.add_detection(self.plant_id, humidity, self.sensor_id)
            self.logging.debug(
                "Added detection [" + str(det_id) + "] - plant_id " + str(self.plant_id) + " - hum: " + str(
                    humidity) + " - sensor: " + str(self.sensor_id))
        else:
            self.logging.warning("Cannot manage this message as a watering ack: [" + str(self.message) + "]")
            raise ValueError("Cannot manage this message as a watering ack")

    def manageWateringAck(self, tokens):
        """
        Confirm a watering action
        :param tokens:
        :return:
        """
        if self.message_values == 2:
            watering_id = int(tokens[1])
            self.go.ack_watering(watering_id)
            self.logging.debug(f"Confirmed watering #{watering_id}")
        else:
            self.logging.warning("Cannot manage this message as a watering ack: [" + str(self.message) + "]")
            raise ValueError("Cannot manage this message as a watering ack")

    # DEPRECATED - It's the old method to manage detection based on the plant di
    def managePlantDetection(self, tokens):
        if self.message_values == 3:
            humidity = int(tokens[1])
            sensor_id = int(tokens[2])
            det_id = self.go.add_detection(self.plant_id, humidity, sensor_id)
            self.logging.debug("Added detection [" + str(det_id) + "] - plant_id " + str(self.plant_id) + " - hum: " + str(humidity) + " - sensor: " + str(sensor_id))
        else:
            self.logging.warning("Cannot manage this message as a detection: [" + str(self.message) + "]")
            raise ValueError("Cannot manage this message as a detection")
