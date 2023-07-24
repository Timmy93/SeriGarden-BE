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

    def install(self):
        """
        Execute the first installation
        :return:
        """
        outcome = self.db.createDB()
        if outcome:
            self.logging.info("Database created")
        else:
            self.logging.warning("Cannot create DB")
        return outcome

    def add_plant(self, plant_name: str, sensor_id: int, owner: str, plant_location: str, plant_type: str):
        return self.db.insertNewPlant(sensor_id, plant_name, owner, plant_location, plant_type)

    def add_detection(self, plant_id: int, humidity: int, sensor_id: int):
        return self.db.insertPlantDetection(plant_id, humidity, sensor_id)

    def add_water(self, plant_id: int, water_quantity: int):
        return self.db.insertPlantWatering(plant_id, water_quantity)

    def getPlantRecap(self):
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
