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
        self._connection = None
        self.test_connection()
        self.dbSemaphore = threading.Condition()

    def _connect(self):
        """
        Try to get DB connection
        :return: The DB connection
        """
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

    def disconnect(self):
        """
        Disconnect from DB
        :return:
        """
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def get_connection(self):
        """
        Retrieve the DB connection
        :return:
        """
        if self._connection is None:
            return self._connect()
        else:
            return self._connection

    def test_connection(self):
        """
        Test the DB connection
        :return:
        """
        self.get_connection()
        self.disconnect()

    def createDB(self):
        """Initialize the DB creating tables and loading missing data"""
        # Create table
        outcome = True
        outcome = outcome and self.createPlantInventory()
        outcome = outcome and self.createPlantHistory()
        outcome = outcome and self.createPlantWaterHistory()
        if outcome:
            self.logging.info("DB setup completed")
        else:
            self.logging.warning("Cannot create DB tables")
        return outcome

    def createTable(self, sql: str):
        con = self.get_connection()
        c = con.cursor()
        try:
            c.execute(sql)
            con.commit()
            c.close()
            return True
        except mariadb.Error as e:
            self.logging.warning("Cannot create database: " + str(e))
            print(f"Error: {e}")
            return False

    def createPlantInventory(self):
        """Create a table containing all the known plant"""
        sql = """CREATE TABLE IF NOT EXISTS """ + self.plant_inventory + """ (
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
        sql = """CREATE TABLE IF NOT EXISTS """ + self.plant_history + """ (
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
        sql = """CREATE TABLE IF NOT EXISTS """ + self.plant_water + """ (
                    watering_id INT auto_increment NOT NULL,
                    plant_id INT NOT NULL,
                    water_quantity INT NOT NULL,
                    watering_done BOOL NOT NULL DEFAULT FALSE,
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT watering_id_PK PRIMARY KEY (watering_id)
                )
                ENGINE=InnoDB
                DEFAULT CHARSET=utf8mb4
                COLLATE=utf8mb4_general_ci
                COMMENT='The last watering done';"""
        return self.createTable(sql)

    def getAllPlantID(self):
        """
        Retrieve the list of all plant ID monitored
        :return:
        """
        sql = """SELECT plant_id
                FROM plant_inventory
                ORDER BY plant_id;
                """
        results = self.getValuesFromDB(sql)
        if len(results) > 0:
            return results
        else:
            self.logging.warning("Cannot retrieve any plant")
            return None

    def getPlantLastDetections(self):
        """
        Retrieve the recap of all plant detection
        :return:
        """
        sql = """SELECT pi2.plant_id, pi2.plant_name, pi2.nodemcu_id, pi2.owner, pi2.plant_location, pi2.plant_type, ph.plant_hum, ph.timestamp as detection_ts, pw.water_quantity, pw.timestamp as watering_ts
                FROM (
                    SELECT *
                    FROM """ + self.plant_history + """ ph 
                    JOIN (
                        SELECT MAX(ph_max.timestamp) AS max_ts
                        FROM """ + self.plant_history + """ ph_max
                        GROUP BY ph_max.plant_id
                        ORDER BY ph_max.plant_id
                    ) tt on ph.timestamp = tt.max_ts
                    ORDER BY ph.plant_id 
                ) ph
                LEFT JOIN (
                    SELECT *
                    FROM """ + self.plant_water + """ pw 
                    JOIN (
                        SELECT MAX(pw_max.timestamp) AS max_ts
                        FROM """ + self.plant_water + """ pw_max
                        GROUP BY pw_max.plant_id
                        ORDER BY pw_max.plant_id
                    ) tt on pw.timestamp = tt.max_ts
                ) pw ON ph.plant_id = pw.plant_id
                RIGHT JOIN """ + self.plant_inventory + """ pi2 ON ph.plant_id=pi2.plant_id 
                GROUP BY pi2.plant_id
                ORDER BY pi2.plant_id
        """
        print(sql)
        results = self.getValuesFromDB(sql)
        if len(results) > 0:
            return results
        else:
            self.logging.warning("Cannot retrieve last detection recap")
            return None

    def getPlantSensorId(self, plant_id):
        """
        Retrieve the current sensor id of this plant
        :param plant_id:
        :return:
        """
        sql = """
            SELECT nodemcu_id
            FROM """ + self.plant_inventory + """
            WHERE plant_id = ?;
        """
        parameters = (plant_id,)
        results = self.getValuesFromDB(sql, parameters)
        if len(results) > 0:
            return results[0]['nodemcu_id']
        else:
            self.logging.warning("This plant [" + str(plant_id) + "] has no sensor registered")
            return None

    def addPlantDetection(self, plant_id: int, humidity: int) -> int | None:
        """
        Add a plant detection - If the plant is missing register the new Plant
        :return: The detection ID
        """
        if not self.knownPlant(plant_id):
            self.logging.warning("Attempting to insert detection for an unknown plant: [" + str(plant_id) + "]")
            return None
        else:
            self.logging.debug("Adding detection for plant ["+str(plant_id)+"]")
            sensor_id = self.getPlantSensorId(plant_id)
            return self.insertPlantDetection(plant_id, humidity, sensor_id)

    def knownPlant(self, plant_id: int) -> bool:
        """
        Check if a plant exists
        :param plant_id: The plant id to check
        :return: The presence of the plant
        """
        sql = """
            SELECT plant_id
            FROM """ + self.plant_inventory + """
            WHERE plant_id = ?;
        """
        parameters = (plant_id,)
        results = self.getValuesFromDB(sql, parameters)
        return len(results) > 0

    def insertNewPlant(self, sensor_id: int, plant_name: str, owner: str, plant_location: str, plant_type: str):
        """
        Register a new plant in the DB
        :param sensor_id: The monitoring sensor ID
        :param plant_type: The plant type
        :param plant_location: The plant location
        :param owner: The plant owner
        :param plant_name: The plant name
        :return: The plant ID
        """
        sql = """INSERT INTO """ + self.plant_inventory + """
                (plant_name, nodemcu_id, owner, plant_location, plant_type)
                VALUES(?, ?, ?, ?, ?);
            """
        values = (plant_name, sensor_id, owner, plant_location, plant_type)
        return self.insertValues(sql, values)

    def insertPlantDetection(self, plant_id: int, humidity: int, sensor_id: int):
        """
        Save a new humidity detection in the DB
        :param sensor_id: The detection sensor
        :param plant_id: The detected plant
        :param humidity: The detected soil humidity
        :return: The detection activity ID
        """
        sql = """INSERT INTO """ + self.plant_history + """
                (plant_id, plant_hum, nodemcu_id)
                VALUES(?, ?, ?);
            """
        values = (plant_id, humidity, sensor_id)
        return self.insertValues(sql, values)

    def insertPlantWatering(self, plant_id: int, water_quantity: int) -> int | None:
        """
        Insert a watering activity in the DB
        :param plant_id: The watered plant
        :param water_quantity: The water quantity
        :return: The watering activity ID
        """
        sql = """INSERT INTO """ + self.plant_water + """
                       (plant_id, water_quantity)
                       VALUES(?, ?);
                   """
        values = (plant_id, water_quantity)
        return self.insertValues(sql, values)

    def ackWatering(self, watering_id: int):
        sql = """UPDATE """ + self.plant_water + """
                       SET watering_done = TRUE
                       WHERE watering_id = ?;
                """
        values = (watering_id, )
        return self.insertValues(sql, values)

    def insertValues(self, insert_query: str, values: tuple):
        """
        Insert the given values inside the DB
        :param insert_query: The insert query to run
        :param values: The values to insert
        :return: The generated ID
        """
        with self.dbSemaphore:
            con = self.get_connection()
            cur = con.cursor()
            cur.execute(insert_query, tuple(values))
            insertion_id = cur.lastrowid
            con.commit()

            # Free DB resources
            cur.close()
            self.disconnect()
            return insertion_id

    def getValuesFromDB(self, sql, values=None):
        with self.dbSemaphore:
            c = self.get_connection().cursor()
            if values:
                c.execute(sql, values)
            else:
                c.execute(sql)
            columns = [item[0] for item in c.description]
            res = c.fetchall()
            # Free DB resources
            c.close()
            self.disconnect()

        # Generate response
        result = []
        for record in res:
            out = {}
            for i, column in enumerate(columns):
                out[column] = record[i]
            result.append(out)
        return result

    def populateDB(self):
        self.logging.warning("Populate to implement")
        return True

    def install(self):
        """
        Install the DB in a single transaction
        :return:
        """
        with self.dbSemaphore:
            outcome = self.createDB()
            outcome = outcome + self.populateDB()
            self.disconnect()
            return outcome
