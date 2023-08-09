import datetime
import logging
import os
import time
import tomllib
import mariadb
import secrets
from Database import Database
from MqttClient import MqttClient
from Scheduler import Scheduler
from astral import LocationInfo, sun


class GardenOrchestrator:

    # Default configuration
    config = {
        "Log": {
            "logFile": "garden.log",
            "logLevel": 'DEBUG'
        },
        "Site": {
            "cors": ["http://localhost:3000"],
            "port": 5001,
            "is_test": True
        },
        "DB": {},
        "MQTT": {}
    }

    def __init__(self):
        self.config = self.load_settings('config.toml')
        self.logging = logging
        self.initialize_log()
        # Connect to DB
        self.db = self.connect_to_db()
        # Connect to MQTT
        try:
            self.mqttc = MqttClient(self.config['MQTT'], self.logging, self)
            self.mqttBroker = MqttClient(self.config['MQTT'], self.logging, self)
            self.mqttc.start()
        except (TimeoutError, ValueError) as e:
            if self.config["Site"].get("is_test", False):
                self.logging.info("Cannot reach MQTT Server - Continuing without MQTT Server ["+str(e)+"]")
            else:
                self.logging.error("Cannot reach MQTT Server ["+str(e)+"]")
                exit(1)

    def setScheduler(self):
        recurrence = self.config['Site'].get('recurrence', 15)
        return Scheduler(self.logging, recurrence, self)

    def install(self):
        """
        Execute the first installation
        :return:
        """
        return self.db.install()

    def getAppSecret(self):
        secret = self.config["Site"].get("SECRET_KEY", None)
        if not secret:
            self.logging.warning("Missing SECRET_KEY - Generating new random SECRET_KEY")
            secret = secrets.token_hex()
        return secret

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

    def add_water(self, plant_id, water_quantity):
        """
        The request to the sensor of plant watering
        :param plant_id:
        :param water_quantity:
        :return:
        """
        self.logging.info("Requesting " + str(water_quantity) + "ml watering to plant [" + str(plant_id) + "]")
        # Parse request.data
        if str(plant_id).isdigit() and str(water_quantity).isdigit():
            # Validating input
            plant_id = int(plant_id)
            water_quantity = int(water_quantity)
            # Adding requested water
            return self.requestWatering(plant_id, water_quantity)
        else:
            self.logging.warning("Invalid input received")
            return None

    def ack_watering(self, watering_id: int):
        return self.db.ackWatering(watering_id)

    def getPlantRecap(self):
        self.logging.debug("Gettin recap")
        status = self.db.getPlantLastDetections()
        return status

    def getPort(self):
        """Retrieve the port for the service"""
        port = self.config.get('Site').get('port') or 5000
        return port

    def load_settings(self, file: str):
        """
        Load setting from toml file
        :return:
        """
        path = os.path.join('Config', file)
        try:
            with open(path, "rb") as f:
                return tomllib.load(f)
        except (OSError, ):
            self.logging.warning("Missing config file - Cannot load configuration")
            return self.config

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
        """Connect to backend DB"""
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

    def evaluateWatering(self):
        # Get action to execute based on time, humidity, default humidity
        actions = self.elaborateWatering()
        # Communicate to sensors to water plant if needed
        used_water = self.transmitActions(actions)
        return {'actions': len(actions), 'water': used_water}

    def elaborateWatering(self):
        """
        This is the core function of the script that elaborate the status of the plant based on several parameters
        :return:
        """
        actions = []
        summary = self.db.getPlantActionSummary()
        for plant_summary in summary:
            # Extract variables
            plant_id = plant_summary.get('plant_id')
            plant_name = plant_summary.get('plant_name')
            default_watering = plant_summary.get('default_watering')
            humidity = plant_summary.get('mean_value')
            lw = plant_summary.get('last_watering_successful') if plant_summary.get('last_watering_successful') is not None else datetime.timedelta(days=30)
            lr = plant_summary.get('last_watering_req') if plant_summary.get('last_watering_req') is not None else datetime.timedelta(days=30)
            last_watering = datetime.datetime.now() - lw
            last_request = datetime.datetime.now() - lr
            # Check watering time and if it's time to re-water
            if self.isWateringTime(plant_summary.get('plant_location')):
                if self.wateringNeeded(humidity):
                    if self.timeToRewater(last_watering, last_request):
                        self.logging.info("Added watering request for: " + plant_name + " #" + str(plant_id))
                        actions.append({'plant_id': plant_id, 'plant_name': plant_name, 'water_quantity': default_watering})
                    else:
                        self.logging.debug("Wait more time before re-watering")
                # else:
                #     self.logging.debug("Watering not needed for: " + plant_name + " #" + str(plant_id) + " [Hum: " + str(humidity) + "%]")
            # else:
            #     self.logging.debug("It is not watering time")
        return actions

    def transmitActions(self, actions: list):
        """
        This function will inform the different sensor if any action is required
        :param actions:
        :return:
        """
        water = 0
        for action in actions:
            # Get parameters
            plant_id = action['plant_id']
            plant_name = action['plant_name']
            water_quantity = action.get('water_quantity', 100)
            # Request watering
            self.logging.info("Requesting " + str(water_quantity) + "ml of water for plant [" + plant_name + "/#" + str(plant_id) + "]")
            if not self.config["Site"].get("is_test", False):
                self.add_water(plant_id, water_quantity)
            else:
                self.logging.info("TEST ENVIRONMENT - Watering not requested")
            water += water_quantity
            time.sleep(self.config['Site'].get('wait_watering', 60))
        return water

    def getAllPlantID(self):
        """
        Retrieve all the plant id in the inventory
        :return:
        """
        values = self.db.getAllPlantID()
        return [d['plant_id'] for d in values]

    def requestWatering(self, plant_id: int, water_quantity: int):
        """Register the watering request and send the MQTT message"""
        watering_id = self.db.insertPlantWatering(plant_id, water_quantity)
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

    def isWateringTime(self, current_location):
        """Check if it's night"""
        start_watering, end_watering = self.getCurrentLocationTimes(current_location)
        # self.logging.debug("Watering time starts:" + str(start_watering) + "- Watering time ends:" + str(end_watering))
        return self.time_in_range(start_watering, end_watering, datetime.datetime.now().time())

    @staticmethod
    def time_in_range(start, end, x):
        """Return true if x is in the range [start, end]"""
        if start <= end:
            return start <= x <= end
        else:
            return start <= x or x <= end

    def getCurrentLocationTimes(self, current_location):
        """From geolocation get information on today sunrise and sunset"""
        default_sunset = datetime.time(23, 0, 0)
        default_sunrise = datetime.time(7, 0, 0)
        if not current_location:
            self.logging.info("Missing plant location - Using default time")
            return default_sunset, default_sunrise
        else:
            try:
                city = LocationInfo(current_location)
                s = sun.sun(city.observer, date=datetime.datetime.now().astimezone())
                sunrise = s['sunrise'].astimezone().time()
                sunset = s['sunset'].astimezone().time()
                return sunset, sunrise
            except:
                self.logging.warning("Cannot retrieve info base on current location - Default value provided")
                return default_sunset, default_sunrise

    @staticmethod
    def timeToRewater(last_watering, last_request):
        """Check if the minimum time between two watering is elapsed"""
        minimum_minutes_between_watering = 120
        minimum_minutes_between_requests = 15
        can_rewater = last_watering < datetime.datetime.now() - datetime.timedelta(minutes=minimum_minutes_between_watering)
        can_request = last_request < datetime.datetime.now() - datetime.timedelta(minutes=minimum_minutes_between_requests)
        return can_rewater and can_request

    @staticmethod
    def wateringNeeded(humidity):
        """Analyse if the plant require a new humidity"""
        # TODO implement an algorithm based on plant type
        return int(humidity) < 50
