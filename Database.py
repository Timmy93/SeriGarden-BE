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
        self.dbSemaphore = threading.Condition()
        self.test_connection()

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
        self.install()

    def create_db(self):
        """Initialize the DB creating tables and loading missing data"""
        # Create table
        outcome = True
        outcome = outcome and self.create_plant_inventory()
        outcome = outcome and self.create_plant_history()
        outcome = outcome and self.create_plant_water_history()
        outcome = outcome and self.optimize_db()
        if outcome:
            self.logging.info("DB setup completed")
        else:
            self.logging.warning("Cannot create DB tables")
        return outcome

    def create_table(self, sql: str):
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

    def create_plant_inventory(self):
        """Create a table containing all the known plant"""
        sql = """CREATE TABLE IF NOT EXISTS """ + self.plant_inventory + """ (
                    plant_id INT auto_increment NOT NULL,
                    plant_name varchar(256) NULL,
                    plant_num INT NULL,
                    nodemcu_id INT NULL,
                    owner varchar(256) NULL,
                    plant_location varchar(256) NULL,
                    plant_type varchar(256) NULL,
                    default_watering int(11) NOT NULL DEFAULT 150 COMMENT 'The default watering value',
                    CONSTRAINT plant_id_PK PRIMARY KEY (plant_id)
                )
                ENGINE=InnoDB
                DEFAULT CHARSET=utf8mb4
                COLLATE=utf8mb4_general_ci
                COMMENT='The list of my plant';"""
        return self.create_table(sql)

    def create_plant_history(self):
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
        return self.create_table(sql)

    def create_plant_water_history(self):
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
        return self.create_table(sql)

    def optimize_db(self):
        """Run the optimization procedure"""
        sql = """CREATE INDEX IF NOT EXISTS idx_plant_history_max_ts_plant_id ON plant_history(plant_id, timestamp DESC);"""
        return self.create_table(sql)

    def get_all_plant_id(self):
        """
        Retrieve the list of all plant ID monitored
        :return:
        """
        sql = """SELECT plant_id
                FROM plant_inventory
                ORDER BY plant_id;
                """
        self.logging.debug("Getting all plant")
        results = self.get_values_from_db(sql)
        if len(results) > 0:
            self.logging.debug(f"Retrieved {len(results)} plants")
            return results
        else:
            self.logging.warning("Cannot retrieve any plant")
            return None

    def get_all_sensor_id(self):
        """
        Retrieve the list of all sensor that have checked in so far
        :return:
        """
        sql = """SELECT DISTINCT nodemcu_id
                FROM plant_inventory
                ORDER BY nodemcu_id;
                """
        self.logging.debug("Getting all sensors")
        results = self.get_values_from_db(sql)
        if len(results) > 0:
            self.logging.debug(f"Retrieved {len(results)} sensors")
            return results
        else:
            self.logging.warning("Cannot retrieve any plant")
            return None

    def get_plant_id(self, sensor_id, plant_num):
        sql = """SELECT plant_id
                FROM """ + self.plant_inventory + """
                WHERE nodemcu_id = ? AND plant_num = ?;
                """
        self.logging.debug(f"Retrieving plant_id of plant #{plant_num} for sensor {sensor_id}")
        parameters = (sensor_id, plant_num)
        results = self.get_values_from_db(sql, parameters)
        if len(results) > 0:
            self.logging.debug(f"Retrieved plant_id #{results[0]['plant_id']}")
            return results[0]['plant_id']
        else:
            self.logging.info(f"This plant {plant_num} has never been tracked by sensor {sensor_id}")
            return None

    def get_plant_id_reference(self, plant_id):
        sql = """SELECT nodemcu_id, plant_num
                FROM """ + self.plant_inventory + """
                WHERE plant_id = ?;
                """
        parameters = (plant_id, )
        self.logging.debug(f"Retrieving sensor and num for plant #{plant_id}")
        results = self.get_values_from_db(sql, parameters)
        if len(results) > 0:
            self.logging.info(f"Retrieved sensor { results[0]['nodemcu_id']} and num {results[0]['plant_num']}")
            return results[0]['nodemcu_id'], results[0]['plant_num']
        else:
            self.logging.warning(f"This plant {plant_id} is not managed by a sensor")
            return None, None

    def get_plant_last_detections(self):
        """
        Retrieve the recap of all plant detection
        :return:
        """
        sql = """SELECT pi2.plant_id, pi2.plant_name, pi2.nodemcu_id, pi2.owner, pi2.plant_location, pi2.plant_type, ph.plant_hum, ph.timestamp as detection_ts, pw.water_quantity, pw.timestamp as watering_ts
                FROM (
                    SELECT ph.plant_id, ph.plant_hum, ph.timestamp
                    FROM """ + self.plant_history + """ ph 
                    JOIN (
                        SELECT ph_max.plant_id, MAX(ph_max.timestamp) AS max_ts
                        FROM """ + self.plant_history + """ ph_max
                        GROUP BY ph_max.plant_id
                        ORDER BY ph_max.plant_id
                    ) tt on ph.timestamp = tt.max_ts AND ph.plant_id = tt.plant_id
                    ORDER BY ph.plant_id 
                ) ph                
                LEFT JOIN (
                    SELECT pw.plant_id, pw.timestamp, pw.water_quantity
                        FROM """ + self.plant_water + """ pw 
                        JOIN (
                            SELECT pw_max.plant_id, MAX(pw_max.timestamp) AS max_ts
                            FROM """ + self.plant_water + """ pw_max
                            GROUP BY pw_max.plant_id
                            ORDER BY pw_max.plant_id
                        ) tt on pw.timestamp = tt.max_ts AND pw.plant_id = tt.plant_id
                    ) pw ON ph.plant_id = pw.plant_id
                RIGHT JOIN """ + self.plant_inventory + """ pi2 ON ph.plant_id=pi2.plant_id 
                GROUP BY pi2.plant_id
                ORDER BY pi2.plant_id
        """
        results = self.get_values_from_db(sql)
        if len(results) > 0:
            self.logging.debug(f"Got recap for {len(results)} plants")
            return results
        else:
            self.logging.warning("Cannot retrieve last detection recap")
            return None

    def get_plant_sensor_id(self, plant_id):
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
        results = self.get_values_from_db(sql, parameters)
        if len(results) > 0:
            return results[0]['nodemcu_id']
        else:
            self.logging.warning("This plant [" + str(plant_id) + "] has no sensor registered")
            return None

    def add_plant_detection(self, plant_id: int, humidity: int) -> int | None:
        """
        Add a plant detection - If the plant is missing register the new Plant
        :return: The detection ID
        """
        if not self.known_plant(plant_id):
            self.logging.warning("Attempting to insert detection for an unknown plant: [" + str(plant_id) + "]")
            return None
        else:
            self.logging.debug("Adding detection for plant ["+str(plant_id)+"]")
            sensor_id = self.get_plant_sensor_id(plant_id)
            return self.insert_plant_detection(plant_id, humidity, sensor_id)

    def known_plant(self, plant_id: int) -> bool:
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
        results = self.get_values_from_db(sql, parameters)
        return len(results) > 0

    def insert_new_plant(self, sensor_id: int, plant_name: str, plant_num: int, owner: str, plant_location: str, plant_type: str):
        """
        Register a new plant in the DB
        :param sensor_id: The monitoring sensor ID
        :param plant_name: The plant name
        :param plant_num: The plant number associated to the given sensor
        :param owner: The plant owner
        :param plant_location: The plant location
        :param plant_type: The plant type
        :return: The plant ID
        """
        sql = """INSERT INTO """ + self.plant_inventory + """
                (plant_name, nodemcu_id, plant_num, owner, plant_location, plant_type)
                VALUES(?, ?, ?, ?, ?, ?);
            """
        values = (plant_name, sensor_id, plant_num, owner, plant_location, plant_type)
        return self.insert_values(sql, values)

    def insert_plant_detection(self, plant_id: int, humidity: int, sensor_id: int):
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
        return self.insert_values(sql, values)

    def insert_plant_watering(self, plant_id: int, water_quantity: int) -> int | None:
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
        return self.insert_values(sql, values)

    def ack_watering(self, watering_id: int):
        """Register the watering confirmation from plant"""
        sql = """UPDATE """ + self.plant_water + """
                       SET watering_done = TRUE
                       WHERE watering_id = ?;
                """
        values = (watering_id, )
        return self.insert_values(sql, values)

    def insert_values(self, insert_query: str, values: tuple):
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

    def get_values_from_db(self, sql, values=None):
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

    def install(self):
        """
        Install the DB in a single transaction
        :return:
        """
        with self.dbSemaphore:
            outcome = self.create_db()
            self.disconnect()
            return outcome

    def get_plant_action_summary(self):
        """Get the humidity status of each plant during last 15 minutes and the last watering"""
        sql = """SELECT ph.plant_id, pi2.plant_name, ROUND(AVG(ph.plant_hum)) AS mean_value, t3.time_elapsed AS last_watering_req, t4.time_elapsed AS last_watering_successful, pi2.default_watering, pi2.plant_location 
                FROM plant_history ph
                LEFT JOIN plant_inventory pi2 ON ph.plant_id = pi2.plant_id 
                LEFT JOIN (
                    SELECT t1.plant_id, t1.water_quantity, TIMEDIFF(NOW(), t1.timestamp) AS time_elapsed
                    FROM plant_water t1
                    JOIN (
                        SELECT plant_id, MAX(timestamp) AS max_timestamp
                        FROM plant_water 
                        GROUP BY plant_id
                    ) t2 ON t1.plant_id = t2.plant_id AND t1.timestamp = t2.max_timestamp
                ) t3 ON t3.plant_id = ph.plant_id
                LEFT JOIN (
                    SELECT t1.plant_id, t1.water_quantity, TIMEDIFF(NOW(), t1.timestamp) AS time_elapsed
                    FROM plant_water t1
                    JOIN (
                        SELECT plant_id, MAX(timestamp) AS max_timestamp
                        FROM plant_water 
                        WHERE watering_done = 1
                        GROUP BY plant_id
                    ) t2 ON t1.plant_id = t2.plant_id AND t1.timestamp = t2.max_timestamp
                ) t4 ON t4.plant_id = ph.plant_id
                WHERE timestamp >= NOW() - INTERVAL 15 MINUTE
                GROUP BY ph.plant_id;"""
        results = self.get_values_from_db(sql)
        return results

    def get_plant_statistics(self, plant_id, duration):
        sql = """SELECT plant_id, ROUND(AVG(plant_hum)) as 'Value', DATE( timestamp ) as 'Date', HOUR( timestamp ) as 'Hour'
            FROM plant_history
            WHERE plant_id = ? AND timestamp > NOW() - INTERVAL ? DAY
            GROUP BY DATE( timestamp ), HOUR( timestamp )"""
        parameters = (int(plant_id), int(duration))
        results = self.get_values_from_db(sql, parameters)
        return results