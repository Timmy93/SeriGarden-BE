import logging

from flask import Flask, url_for, redirect, jsonify, request
from flask_cors import CORS
from waitress import serve

from GardenOrchestrator import GardenOrchestrator


def main():
    go = GardenOrchestrator()
    #Start scheduler
    scheduler = go.setScheduler()
    scheduler.start()
    #Start website
    app = Flask(__name__)
    app.config['SECRET_KEY'] = go.getAppSecret()
    app.config['CORS_HEADERS'] = 'Content-Type'
    CORS(app,
         origins=go.getAllowedCorsSites(),
         expose_headers=["Content-Disposition"],
         allow_headers=["Content-Type", "Accept"],
         methods=['GET', 'POST', 'OPTIONS']
         )

    @app.route("/")
    def home():
        return redirect(url_for('show_status'))

    @app.route("/status")
    def show_status():
        res = go.getPlantRecap()
        return jsonify(res)

    @app.route("/statistic/daily/<plant_id>", methods=['GET'])
    def get_daily_statistics(plant_id):
        duration = 1
        res = go.getPlantStatistics(plant_id, duration)
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

    @app.route("/add/water/<plant_id>", methods=['GET'])
    def add_water(plant_id):
        water_quantity = "200"
        return generic_add_water(plant_id, water_quantity)

    @app.route("/add/water/<plant_id>", methods=['POST'])
    def add_water_post(plant_id):
        try:
            print(request.data)
            data = request.get_json()
            water_quantity = data['quantity']
            return generic_add_water(plant_id, water_quantity)
        except:
            return jsonify("Cannot add watering for plant [" + str(plant_id) + "]")

    def generic_add_water(plant_id, water_quantity):
        response = {
            "plant_id": plant_id,
            "water_requested": water_quantity,
            "success": False,
            "message": ""
        }
        if go.add_water(plant_id, water_quantity):
            response["success"] = True
        else:
            response["message"] = "Cannot add watering for this plant"
        return jsonify(response)

    @app.route("/add/detection")
    def add_detection():
        plant_id = 1
        sensor_id = 1
        humidity = 56
        if go.add_detection(plant_id, humidity, sensor_id):
            return jsonify("Added detection [" + str(plant_id) + "]")
        else:
            return jsonify("Cannot add detection for plant [" + str(plant_id) + "]")

    #Start webserver
    serve(app, host='0.0.0.0', port=go.getPort())


if __name__ == '__main__':
    main()
