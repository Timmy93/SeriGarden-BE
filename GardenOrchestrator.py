import logging
import os
import tomllib
import mariadb
from Database import Database
from MqttClient import MqttClient


class GardenOrchestrator:

    def __init__(self):
        self.config = self.load_settings('config.toml')
        self.logging = logging
        self.initialize_log()
        # Connect to DB
        self.db = self.connect_to_db()
        # Connect to MQTT
        self.mqttc = MqttClient(self.config['MQTT'], self.logging, self)
        self.mqttBroker = MqttClient(self.config['MQTT'], self.logging, self)
        self.mqttc.start()

    def install(self):
        """
        Execute the first installation
        :return:
        """
        return self.db.install()

    def add_plant(self, plant_name: str, sensor_id: int, owner: str, plant_location: str, plant_type: str):
        """
        Insert a new plant in the DB
        :param plant_name: The plant name
        :param sensor_id: The sensor that is managing the plant
        :param owner: The plant owner
        :param plant_location: The plant location
        :param plant_type: The plant type
        :return:
        """
        return self.db.insertNewPlant(sensor_id, plant_name, owner, plant_location, plant_type)

    def add_detection(self, plant_id: int, humidity: int, sensor_id: int):
        """
        Insert a humidity detection received from a sensor
        :param plant_id: The monitored plant
        :param humidity: The detected humidity
        :param sensor_id: The sensor that is sending the measure
        :return:
        """
        self.logging.debug("Adding detection")
        return self.db.insertPlantDetection(plant_id, humidity, sensor_id)

    def add_water(self, plant_id: int, water_quantity: int):
        """
        The request to the sensor of plant watering
        :param plant_id:
        :param water_quantity:
        :return:
        """
        self.logging.info("Requesting " + str(water_quantity) + "ml watering to plant [" + str(plant_id) + "]")
        watering_id = self.db.insertPlantWatering(plant_id, water_quantity)
        return self.requestWatering(plant_id, water_quantity, watering_id)

    def ack_watering(self, watering_id: int):
        return self.db.ackWatering(watering_id)

    def getPlantRecap(self):
        self.logging.debug("Gettin recap")
        status = self.db.getPlantLastDetections()
        return status

    def getPlantActions(self):
        # TODO Implement automatic watering request
        actions = self.elaborateWatering()
        self.trasmitActions(actions)
        return actions

    def getPort(self):
        port = self.config.get('Site').get('port') or 5000
        return port

    @staticmethod
    def load_settings(file: str):
        """
        Load setting from toml file
        :return:
        """
        path = os.path.join('Config', file)
        with open(path, "rb") as f:
            return tomllib.load(f)

    def initialize_log(self):
        """
        Initialize log file
        :return:
        """
        filename = os.path.join('Config', self.config['Log']['logFile'])
        self.logging.basicConfig(
            filename=filename,
            level=self.config['Log']['logLevel'],
            format='%(asctime)s %(levelname)-8s %(message)s')
        self.logging.info("Garden Sericloud - Started")

    def connect_to_db(self):
        """
        Connect to backend DB
        :return:
        """
        try:
            return Database(self.config['DB'], self.logging)
        except mariadb.Error as e:
            self.logging.error("Cannot connect to DB [" + str(e) + "]")
            print("Cannot connect to DB [" + str(e) + "]")
            exit(1)

    def getAllowedCorsSites(self):
        allowed = self.config.get('Site').get('cors') or ['http://localhost']
        self.logging.debug("CORS allowed: " + str(allowed))
        return allowed

    def elaborateWatering(self):
        """
        This is the core function of the script that elaborate the status of the plant based on several parameters
        :return:
        """
        status = self.db.getPlantLastDetections()
        #TODO Figure out the controls to implement
        return {}

    def trasmitActions(self, actions):
        """
        This function will inform the different sensor if any action is required
        :param actions:
        :return:
        """
        # TODO Implement using MQTT
        pass

    def getAllPlantID(self):
        """
        Retrieve all the plant id in the inventory
        :return:
        """
        values = self.db.getAllPlantID()
        return [d['plant_id'] for d in values]

    def populateDB(self):
        self.logging.warning("Populate to implement")
        return True

    def requestWatering(self, plant_id: int, water_quantity: int, watering_id: int):
        water_time = self.elaborateWaterTime(water_quantity)
        return self.mqttBroker.send_message("water/" + str(plant_id), "w_"+str(watering_id)+"_"+str(water_time))

    @staticmethod
    def elaborateWaterTime(water_quantity):
        """
        Function used to calculate the watering time for a given quantity
        :param water_quantity:
        :return:
        """
        # TODO Measure real values - Empiric measure
        # water_quantity = (water_time - initial_delta) * flow_rate
        # Increase to wait more time before starting to count water
        initial_delta = 300
        # Reduce flow_rate to increase the watering time
        flow_rate = 1/34.26
        water_time = round(water_quantity/flow_rate + initial_delta)
        return water_time

