import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from unittest.mock import MagicMock, patch
from socket import gaierror
from MqttClient import MqttClient

@pytest.fixture
def mock_mqtt_client():
    with patch("MqttClient.mqtt.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        yield mock_client


@pytest.fixture
def mqtt_instance(mock_mqtt_client):
    mock_log = MagicMock()
    mock_go = MagicMock()
    config = {"host": "localhost", "port": 1883, "keepalive": 60}
    return MqttClient(config, mock_log, mock_go)


def test_subscribe_calls_client_and_logs(mqtt_instance):
    client = MagicMock()
    topic = "sensor/42"

    mqtt_instance.subscribe(client, topic)

    client.subscribe.assert_called_with(topic)
    mqtt_instance.logging.info.assert_called_with(f"Subscribed to topic : {topic}")


def test_on_connect_subscribes_all(mqtt_instance):
    client = MagicMock()
    mqtt_instance.go.get_all_sensor_id = MagicMock(return_value=[1, 2, 3])

    mqtt_instance.on_connect(client, None, None, 0)

    client.subscribe.assert_any_call("greeting")
    client.subscribe.assert_any_call("sensor/1")
    client.subscribe.assert_any_call("sensor/2")
    client.subscribe.assert_any_call("sensor/3")


def test_new_subscription_calls_client(mqtt_instance):
    topic = "dynamic/topic"
    mqtt_instance.client = MagicMock()

    mqtt_instance.new_subscription(topic)

    mqtt_instance.client.subscribe.assert_called_with(topic)


def test_send_message_success(mqtt_instance, mock_mqtt_client):
    result = mqtt_instance.send_message("my/topic", "hello")

    mock_mqtt_client.connect.assert_called_with(
        "localhost", 1883, 60
    )
    mock_mqtt_client.publish.assert_called_with("my/topic", "hello")
    mock_mqtt_client.disconnect.assert_called_once()
    assert result is True


def test_send_message_connection_refused(mqtt_instance, mock_mqtt_client):
    mock_mqtt_client.connect.side_effect = ConnectionRefusedError()

    result = mqtt_instance.send_message("my/topic", "hello")

    assert result is False
    mqtt_instance.logging.warning.assert_called()
    mock_mqtt_client.disconnect.assert_not_called()


def test_send_message_host_not_found(mqtt_instance, mock_mqtt_client):
    mock_mqtt_client.connect.side_effect = gaierror()

    result = mqtt_instance.send_message("my/topic", "hello")

    assert result is False
    mqtt_instance.logging.error.assert_called_with("Host: [localhost] not found")


def test_on_message_triggers_handler(mqtt_instance):
    mock_message = MagicMock()
    mock_message.payload.decode.return_value = "payload"
    mock_message.topic = "test/topic"

    with patch("MqttClient.MessageHandler") as mock_handler:
        instance = mock_handler.return_value

        mqtt_instance.on_message(None, None, mock_message)

        mqtt_instance.logging.info.assert_called_with("Received message")
        mock_handler.assert_called_with(
            mqtt_instance.logging, "payload", "test/topic", mqtt_instance.go
        )
        instance.start.assert_called_once()