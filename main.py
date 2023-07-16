from flask import Flask, url_for, redirect, jsonify
from flask_cors import CORS

from GardenOrchestrator import GardenOrchestrator


def main():
    ao = GardenOrchestrator()
    app = Flask(__name__)
    CORS(app, origins=ao.getAllowedCorsSites(), expose_headers=["Content-Disposition"])

    @app.route("/")
    def home():
        return redirect(url_for('show_status'))

    @app.route("/status")
    def show_status():
        res = ao.getPlantRecap()
        return jsonify(res)

    @app.route("/install")
    def install():
        if ao.install():
            return jsonify("Database installed")
        else:
            return jsonify("Cannot setup database")

    app.run(host='0.0.0.0', port=ao.getPort())


if __name__ == '__main__':
    main()
