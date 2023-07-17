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
        self.mqttc = MqttClient(self.config, self.logging, self.db)

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
        self.logging.basicConfig(
            filename=self.config['Log']['logFile'],
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
        return

    def trasmitActions(self, actions):
        """
        This function will inform the different sensor if any action is required
        :param actions:
        :return:
        """
        pass
