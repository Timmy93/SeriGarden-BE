import logging

from flask import Flask, url_for, redirect, jsonify
from flask_cors import CORS

from GardenOrchestrator import GardenOrchestrator


def main():
    go = GardenOrchestrator()
    app = Flask(__name__)
    CORS(app, origins=go.getAllowedCorsSites(), expose_headers=["Content-Disposition"])

    @app.route("/")
    def home():
        return redirect(url_for('show_status'))

    @app.route("/status")
    def show_status():
        res = go.getPlantRecap()
        return jsonify(res)

    @app.route("/install")
    def install():
        if go.install():
            return jsonify("OK - Database installed")
        else:
            return jsonify("KO - Cannot setup database")

    @app.route("/add/plant")
    def add_plant():
        plant_name = "test222"
        sensor_id = 1
        owner = "AAAA"
        plant_location = "Roma"
        plant_type = "Fragola"
        if go.add_plant(plant_name, sensor_id, owner, plant_location, plant_type):
            return jsonify("Added plant[" + str(plant_name) + "]")
        else:
            logging.warning("Cannot insert this plant [" + str(plant_name) + "]")
            return jsonify("Cannot insert this plant")

    @app.route("/add/water")
    def add_water():
        plant_id = 1
        water_quantity = 100
        if go.add_water(plant_id, water_quantity):
            return jsonify("Added watering [" + str(plant_id) + "]")
        else:
            return jsonify("Cannot add watering for plant [" + str(plant_id) + "]")

    @app.route("/add/detection")
    def add_detection():
        plant_id = 1
        sensor_id = 1
        humidity = 56
        if go.add_detection(plant_id, humidity, sensor_id):
            return jsonify("Added detection [" + str(plant_id) + "]")
        else:
            return jsonify("Cannot add detection for plant [" + str(plant_id) + "]")

    app.run(host='0.0.0.0', port=go.getPort())


if __name__ == '__main__':
    main()
