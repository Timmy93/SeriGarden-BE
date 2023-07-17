import logging
import mariadb
import threading


class Database:
    plant_inventory = "plant_inventory"
    plant_history = "plant_history"
    plant_water = "plant_water"

    def __init__(self, config, log: logging):
        self.config = config
        self.logging = log
        self.connection = self.connect()
        self.dbSemaphore = threading.Condition()

    def connect(self):
        try:
            conn = mariadb.connect(
                user=self.config.get('db_user'),
                password=self.config.get('db_password'),
                host=self.config.get('db_host'),
                port=self.config.get('db_port'),
                database=self.config.get('db_name')
            )
            return conn
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            self.logging.error("Cannot log in to MariaDB")
            raise e

    def createDB(self):
        """Initialize the DB creating tables and loading missing data"""
        # Create table
        outcome = True
        outcome = outcome and self.createPlantInventory()
        outcome = outcome and self.createPlantHistory()
        outcome = outcome and self.createPlantWaterHistory()
        return outcome

    def createTable(self, sql: str):
        c = self.connection.cursor()
        try:
            c.execute(sql)
            return True
        except mariadb.Error as e:
            self.logging.warning("Cannot create database: " + str(e))
            print(f"Error: {e}")
            return False

    def createPlantInventory(self):
        """Create a table containing all the known plant"""
        sql = """CREATE TABLE """ + self.plant_inventory + """ (
                    plant_id INT auto_increment NOT NULL,
                    plant_name varchar(256) NULL,
                    nodemcu_id INT NULL,
                    owner varchar(256) NULL,
                    plant_location varchar(256) NULL,
                    plant_type varchar(256) NULL,
                    CONSTRAINT plant_id_PK PRIMARY KEY (plant_id)
                )
                ENGINE=InnoDB
                DEFAULT CHARSET=utf8mb4
                COLLATE=utf8mb4_general_ci
                COMMENT='The list of my plant';"""
        return self.createTable(sql)

    def createPlantHistory(self):
        """Create a table containing all the plant status detection"""
        sql = """CREATE TABLE """ + self.plant_history + """ (
                    detection_id INT auto_increment NOT NULL,
                    plant_id INT NOT NULL,
                    plant_hum INT NOT NULL,
                    nodemcu_id INT NULL,
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT detection_id_PK PRIMARY KEY (detection_id)
                )
                ENGINE=InnoDB
                DEFAULT CHARSET=utf8mb4
                COLLATE=utf8mb4_general_ci
                COMMENT='The last detections of plant soil humidity';"""
        return self.createTable(sql)

    def createPlantWaterHistory(self):
        """Create a table containing all the last watering done"""
        sql = """CREATE TABLE """ + self.plant_water + """ (
                    watering_id INT auto_increment NOT NULL,
                    plant_id INT NOT NULL,
                    water_quantity INT NOT NULL,
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT watering_id_PK PRIMARY KEY (watering_id)
                )
                ENGINE=InnoDB
                DEFAULT CHARSET=utf8mb4
                COLLATE=utf8mb4_general_ci
                COMMENT='The last watering done';"""
        return self.createTable(sql)

    def getPlantID(self):
        """
        Retrieve the list of all plant ID monitored
        :return:
        """
        pass

    def getPlantLastDetections(self):
        """
        Retrieve the recap of all plant detection
        :return:
        """
        pass

    def addPlantDetection(self, plant_id: int, humidity: int):
        """
        Add a plant detection - If the plant is missing register the new Plant
        :return:
        """
        if not self.knownPlant(plant_id):
            self.insertNewPlant(plant_id)
        self.insertPlantDetection(plant_id, humidity)

    def knownPlant(self, plant_id: int) -> bool:
        """
        Check if a plant exists
        :param plant_id: The plant id to check
        :return: The presence of the plant
        """
        return True

    def insertNewPlant(self, plant_id: int):
        """
        Register a new plant in the DB
        :param plant_id: The plant id
        :return:
        """
        pass

    def insertPlantDetection(self, plant_id: int, humidity: int):
        """
        Save a new humidity detection in the DB
        :param plant_id:
        :param humidity:
        :return:
        """
        pass