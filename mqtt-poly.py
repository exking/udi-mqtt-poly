#!/usr/bin/env python3

import polyinterface
import sys
import logging
import paho.mqtt.client as mqtt
import json
import yaml

LOGGER = polyinterface.LOGGER


class Controller(polyinterface.Controller):
    def __init__(self, polyglot):
        super().__init__(polyglot)
        self.name = "MQTT Controller"
        self.address = "mqctrl"
        self.primary = self.address
        self.mqtt_server = "localhost"
        self.mqtt_port = 1883
        self.mqtt_user = None
        self.mqtt_password = None
        self.devlist = None
        # example: [ {'id': 'sonoff1', 'type': 'switch', 'status_topic': 'stat/sonoff1/power', 'cmd_topic': 'cmnd/sonoff1/power'} ]
        self.status_topics = []
        self.mqttc = None

    def start(self):
        # LOGGER.setLevel(logging.INFO)
        LOGGER.info("Started MQTT controller")
        if "mqtt_server" in self.polyConfig["customParams"]:
            self.mqtt_server = self.polyConfig["customParams"]["mqtt_server"]
        if "mqtt_port" in self.polyConfig["customParams"]:
            self.mqtt_port = int(self.polyConfig["customParams"]["mqtt_port"])
        if "mqtt_user" not in self.polyConfig["customParams"]:
            LOGGER.error("mqtt_user must be configured")
            return False
        if "mqtt_password" not in self.polyConfig["customParams"]:
            LOGGER.error("mqtt_password must be configured")
            return False

        self.mqtt_user = self.polyConfig["customParams"]["mqtt_user"]
        self.mqtt_password = self.polyConfig["customParams"]["mqtt_password"]

        if "devfile" in self.polyConfig["customParams"]:
            try:
                f = open(self.polyConfig["customParams"]["devfile"])
            except Exception as ex:
                LOGGER.error(
                    "Failed to open {}: {}".format(
                        self.polyConfig["customParams"]["devfile"], ex
                    )
                )
                return False
            try:
                data = yaml.safe_load(f.read())
                f.close()
            except Exception as ex:
                LOGGER.error(
                    "Failed to parse {} content: {}".format(
                        self.polyConfig["customParams"]["devfile"], ex
                    )
                )
                return False

            if "devices" not in data:
                LOGGER.error(
                    "Manual discovery file {} is missing bulbs section".format(
                        self.polyConfig["customParams"]["devfile"]
                    )
                )
                return False
            self.devlist = data["devices"]

        elif "devlist" in self.polyConfig["customParams"]:
            try:
                self.devlist = json.loads(self.polyConfig["customParams"]["devlist"])
            except Exception as ex:
                LOGGER.error("Failed to parse the devlist: {}".format(ex))
                return False
        else:
            LOGGER.error("devlist must be configured")
            return False

        self.mqttc = mqtt.Client()
        self.mqttc.on_connect = self._on_connect
        self.mqttc.on_disconnect = self._on_disconnect
        self.mqttc.on_message = self._on_message
        self.mqttc.is_connected = False

        for dev in self.devlist:
            if (
                "id" not in dev
                or "status_topic" not in dev
                or "cmd_topic" not in dev
                or "type" not in dev
            ):
                LOGGER.error("Invalid device definition: {}".format(json.dumps(dev)))
                continue
            if "name" in dev:
                name = dev["name"]
            else:
                name = dev["id"]
            address = dev["id"].lower().replace("_", "")[:14]
            if dev["type"] == "switch":
                if not address is self.nodes:
                    LOGGER.info("Adding {} {}".format(dev["type"], name))
                    self.addNode(MQSwitch(self, self.address, address, name, dev))
                    self.status_topics.append(dev["status_topic"])
            elif dev["type"] == "sensor":
                if not address is self.nodes:
                    LOGGER.info("Adding {} {}".format(dev["type"], name))
                    self.addNode(MQSensor(self, self.address, address, name, dev))
                    self.status_topics.append(dev["status_topic"])
            elif dev["type"] == "flag":
                if not address is self.nodes:
                    LOGGER.info("Adding {} {}".format(dev["type"], name))
                    self.addNode(MQFlag(self, self.address, address, name, dev))
                    self.status_topics.append(dev["status_topic"])
            elif dev["type"] == "TempHumid":
                if not address is self.nodes:
                    LOGGER.info("Adding {} {}".format(dev["type"], name))
                    self.addNode(MQdht(self, self.address, address, name, dev))
                    self.status_topics.append(dev["status_topic"])
            elif dev["type"] == "Temp":
                if not address is self.nodes:
                    LOGGER.info("Adding {} {}".format(dev["type"], name))
                    self.addNode(MQds(self, self.address, address, name, dev))
                    self.status_topics.append(dev["status_topic"])
            elif dev["type"] == "TempHumidPress":
                if not address is self.nodes:
                    LOGGER.info("Adding {} {}".format(dev["type"], name))
                    self.addNode(MQbme(self, self.address, address, name, dev))
                    self.status_topics.append(dev["status_topic"])
            elif dev["type"] == "distance":
                if not address is self.nodes:
                    LOGGER.info("Adding {} {}".format(dev["type"], name))
                    self.addNode(MQhcsr(self, self.address, address, name, dev))
                    self.status_topics.append(dev["status_topic"])
            elif dev["type"] == "analog":
                if not address is self.nodes:
                    LOGGER.info("Adding {} {}".format(dev["type"], name))
                    self.addNode(MQAnalog(self, self.address, address, name, dev))
                    self.status_topics.append(dev["status_topic"])
            elif dev["type"] == "s31":
                if not address is self.nodes:
                    LOGGER.info("Adding {} {}".format(dev["type"], name))
                    self.addNode(MQs31(self, self.address, address, name, dev))
                    self.status_topics.append(dev["status_topic"])
            elif dev["type"] == "raw":
                if not address is self.nodes:
                    LOGGER.info("Adding {} {}".format(dev["type"], name))
                    self.addNode(MQraw(self, self.address, address, name, dev))
                    self.status_topics.append(dev["status_topic"])
            elif dev["type"] == "RGBW":
                if not address is self.nodes:
                    LOGGER.info("Adding {} {}".format(dev["type"], name))
                    self.addNode(MQRGBWstrip(self, self.address, address, name, dev))
                    self.status_topics.append(dev["status_topic"])
            elif dev["type"] == "ifan":
                if not address is self.nodes:
                    LOGGER.info("Adding {} {}".format(dev["type"], name))
                    self.addNode(MQFan(self, self.address, address, name, dev))
                    self.status_topics.append(dev["status_topic"])
            else:
                LOGGER.error("Device type {} is not yet supported".format(dev["type"]))
        LOGGER.info("Done adding nodes, connecting to MQTT broker...")
        self.mqttc.username_pw_set(self.mqtt_user, self.mqtt_password)
        try:
            self.mqttc.connect(self.mqtt_server, self.mqtt_port, 10)
            self.mqttc.loop_start()
        except Exception as ex:
            LOGGER.error("Error connecting to Poly MQTT broker {}".format(ex))
            return False

        return True

    def _on_connect(self, mqttc, userdata, flags, rc):
        if rc == 0:
            LOGGER.info("Poly MQTT Connected, subscribing...")
            self.mqttc.is_connected = True
            results = []
            for stopic in self.status_topics:
                results.append((stopic, tuple(self.mqttc.subscribe(stopic))))
            for (topic, (result, mid)) in results:
                if result == 0:
                    LOGGER.info(
                        "Subscribed to {} MID: {}, res: {}".format(topic, mid, result)
                    )
                else:
                    LOGGER.error(
                        "Failed to subscribe {} MID: {}, res: {}".format(
                            topic, mid, result
                        )
                    )
            for node in self.nodes:
                if self.nodes[node].address != self.address:
                    self.nodes[node].query()
        else:
            LOGGER.error("Poly MQTT Connect failed")

    def _on_disconnect(self, mqttc, userdata, rc):
        self.mqttc.is_connected = False
        if rc != 0:
            LOGGER.warning("Poly MQTT disconnected, trying to re-connect")
            try:
                self.mqttc.reconnect()
            except Exception as ex:
                LOGGER.error("Error connecting to Poly MQTT broker {}".format(ex))
                return False
        else:
            LOGGER.info("Poly MQTT graceful disconnection")

    def _on_message(self, mqttc, userdata, message):
        topic = message.topic
        payload = message.payload.decode("utf-8")
        LOGGER.debug("Received {} from {}".format(payload, topic))
        try:
            self.nodes[self._dev_by_topic(topic)].updateInfo(payload)
        except Exception as ex:
            LOGGER.error("Failed to process message {}".format(ex))

    def _dev_by_topic(self, topic):
        for dev in self.devlist:
            if dev["status_topic"] == topic:
                return dev["id"].lower()[:14]
        return None

    def mqtt_pub(self, topic, message):
        self.mqttc.publish(topic, message, retain=False)

    def stop(self):
        self.mqttc.loop_stop()
        self.mqttc.disconnect()
        LOGGER.info("MQTT is stopping")

    def updateInfo(self):
        pass

    def query(self, command=None):
        for node in self.nodes:
            self.nodes[node].reportDrivers()

    def discover(self, command=None):
        pass

    id = "MQCTRL"
    commands = {"DISCOVER": discover}
    drivers = [{"driver": "ST", "value": 1, "uom": 2}]


class MQSwitch(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.cmd_topic = device["cmd_topic"]
        self.on = False

    def start(self):
        pass

    def updateInfo(self, payload):
        if payload == "ON":
            if not self.on:
                self.reportCmd("DON")
                self.on = True
            self.setDriver("ST", 100)
        elif payload == "OFF":
            if self.on:
                self.reportCmd("DOF")
                self.on = False
            self.setDriver("ST", 0)
        else:
            LOGGER.error("Invalid payload {}".format(payload))

    def set_on(self, command):
        self.on = True
        self.controller.mqtt_pub(self.cmd_topic, "ON")

    def set_off(self, command):
        self.on = False
        self.controller.mqtt_pub(self.cmd_topic, "OFF")

    def query(self, command=None):
        self.controller.mqtt_pub(self.cmd_topic, "")
        self.reportDrivers()

    drivers = [{"driver": "ST", "value": 0, "uom": 78}]

    id = "MQSW"
    hint = [4, 2, 0, 0]
    commands = {"QUERY": query, "DON": set_on, "DOF": set_off}


class MQFan(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.cmd_topic = device["cmd_topic"]
        self.fan_speed = 0

    def start(self):
        pass

    def updateInfo(self, payload):
        try:
            json_payload = json.loads(payload)
            fan_speed = int(json_payload['FanSpeed'])
        except Exception as ex:
            LOGGER.error(f"Could not decode payload {payload}: {ex}")
        if 4 < fan_speed < 0:
            LOGGER.error(f"Unexpected Fan Speed {fan_speed}")
            return
        if self.fan_speed == 0 and fan_speed > 0:
            self.reportCmd("DON")
        if self.fan_speed > 0 and fan_speed == 0:
            self.reportCmd("DOF")
        self.fan_speed = fan_speed
        self.setDriver("ST", self.fan_speed)

    def set_on(self, command):
        try:
            self.fan_speed = int(command.get('value'))
        except Exception as ex:
            LOGGER.info(f"Unexpected Fan Speed {ex}, assuming High")
            self.fan_speed = 3
        if 4 < self.fan_speed < 0:
            LOGGER.error(f"Unexpected Fan Speed {self.fan_speed}, assuming High")
            self.fan_speed = 3
        self.setDriver("ST", self.fan_speed)
        self.controller.mqtt_pub(self.cmd_topic, self.fan_speed)

    def set_off(self, command):
        self.fan_speed = 0
        self.setDriver("ST", self.fan_speed)
        self.controller.mqtt_pub(self.cmd_topic, self.fan_speed)

    def set_low(self, command):
        self.fan_speed = 1
        self.setDriver("ST", self.fan_speed)
        self.controller.mqtt_pub(self.cmd_topic, self.fan_speed)

    def set_med(self, command):
        self.fan_speed = 2
        self.setDriver("ST", self.fan_speed)
        self.controller.mqtt_pub(self.cmd_topic, self.fan_speed)

    def set_high(self, command):
        self.fan_speed = 3
        self.setDriver("ST", self.fan_speed)
        self.controller.mqtt_pub(self.cmd_topic, self.fan_speed)
        
    def speed_up(self, command):
        self.controller.mqtt_pub(self.cmd_topic, "+")

    def speed_down(self, command):
        self.controller.mqtt_pub(self.cmd_topic, "-")

    def query(self, command=None):
        self.controller.mqtt_pub(self.cmd_topic, "")
        self.reportDrivers()

    drivers = [{"driver": "ST", "value": 0, "uom": 25}]

    id = "MQFAN"
    hint = [4, 2, 0, 0]
    commands = {"QUERY": query, "DON": set_on, "DOF": set_off, "FDUP": speed_up, "FDDOWN": speed_down}


class MQSensor(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.cmd_topic = device["cmd_topic"]
        self.on = False
        self.motion = False

    def start(self):
        pass

    def updateInfo(self, payload):
        try:
            data = json.loads(payload)
        except Exception as ex:
            LOGGER.error(
                "Failed to parse MQTT Payload as Json: {} {}".format(ex, payload)
            )
            return False

        # motion detector
        if "motion" in data:
            if data["motion"] == "standby":
                self.setDriver("ST", 0)
                if self.motion:
                    self.motion = False
                    self.reportCmd("DOF")
            else:
                self.setDriver("ST", 1)
                if not self.motion:
                    self.motion = True
                    self.reportCmd("DON")
        else:
            self.setDriver("ST", 0)
        # temperature
        if "temperature" in data:
            self.setDriver("CLITEMP", data["temperature"])
        # heatIndex
        if "heatIndex" in data:
            self.setDriver("GPV", data["heatIndex"])
        # humidity
        if "humidity" in data:
            self.setDriver("CLIHUM", data["humidity"])
        # light detecor reading
        if "ldr" in data:
            self.setDriver("LUMIN", data["ldr"])
        # LED
        if "state" in data:
            # LED is present
            if data["state"] == "ON":
                self.setDriver("GV0", 100)
            else:
                self.setDriver("GV0", 0)
            if "brightness" in data:
                self.setDriver("GV1", data["brightness"])
            if "color" in data:
                if "r" in data["color"]:
                    self.setDriver("GV2", data["color"]["r"])
                if "g" in data["color"]:
                    self.setDriver("GV3", data["color"]["g"])
                if "b" in data["color"]:
                    self.setDriver("GV4", data["color"]["b"])

    def led_on(self, command):
        self.controller.mqtt_pub(self.cmd_topic, json.dumps({"state": "ON"}))

    def led_off(self, command):
        self.controller.mqtt_pub(self.cmd_topic, json.dumps({"state": "OFF"}))

    def led_set(self, command):
        query = command.get("query")
        red = self._check_limit(int(query.get("R.uom100")))
        green = self._check_limit(int(query.get("G.uom100")))
        blue = self._check_limit(int(query.get("B.uom100")))
        brightness = self._check_limit(int(query.get("I.uom100")))
        transition = int(query.get("D.uom58"))
        flash = int(query.get("F.uom58"))
        cmd = {
            "state": "ON",
            "brightness": brightness,
            "color": {"r": red, "g": green, "b": blue},
        }
        if transition > 0:
            cmd["transition"] = transition
        if flash > 0:
            cmd["flash"] = flash

        self.controller.mqtt_pub(self.cmd_topic, json.dumps(cmd))

    def _check_limit(self, value):
        if value > 255:
            return 255
        elif value < 0:
            return 0
        else:
            return value

    def query(self, command=None):
        self.reportDrivers()

    drivers = [
        {"driver": "ST", "value": 0, "uom": 2},
        {"driver": "CLITEMP", "value": 0, "uom": 17},
        {"driver": "GPV", "value": 0, "uom": 17},
        {"driver": "CLIHUM", "value": 0, "uom": 22},
        {"driver": "LUMIN", "value": 0, "uom": 36},
        {"driver": "GV0", "value": 0, "uom": 78},
        {"driver": "GV1", "value": 0, "uom": 100},
        {"driver": "GV2", "value": 0, "uom": 100},
        {"driver": "GV3", "value": 0, "uom": 100},
        {"driver": "GV4", "value": 0, "uom": 100},
    ]

    id = "MQSENS"

    commands = {"QUERY": query, "DON": led_on, "DOF": led_off, "SETLED": led_set}

    # this is meant as a flag for if you have a sensor or condition on your IOT device
    # which you want the device program rather than the ISY to flag
    # FLAG-0 = OK
    # FLAG-1 = NOK
    # FLAG-2 = LO
    # FLAG-3 = HI
    # FLAG-4 = ERR
    # FLAG-5 = IN
    # FLAG-6 = OUT
    # FLAG-7 = UP
    # FLAG-8 = DOWN
    # FLAG-9 = TRIGGER
    # FLAG-10 = ON
    # FLAG-11 = OFF
    # FLAG-12 = ---
    # payload is direct (like SW) not JSON encoded (like SENSOR)
    # example device: liquid float {OK, LO, HI}
    # example condition: IOT devices sensor connections {OK, NOK, ERR(OR)}


class MQFlag(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.cmd_topic = device["cmd_topic"]

    def start(self):
        pass

    def updateInfo(self, payload):
        if payload == "OK":
            self.setDriver("ST", 0)
        elif payload == "NOK":
            self.setDriver("ST", 1)
        elif payload == "LO":
            self.setDriver("ST", 2)
        elif payload == "HI":
            self.setDriver("ST", 3)
        elif payload == "IN":
            self.setDriver("ST", 5)
        elif payload == "OUT":
            self.setDriver("ST", 6)
        elif payload == "UP":
            self.setDriver("ST", 7)
        elif payload == "DOWN":
            self.setDriver("ST", 8)
        elif payload == "TRIGGER":
            self.setDriver("ST", 9)
        elif payload == "ON":
            self.setDriver("ST", 10)
        elif payload == "OFF":
            self.setDriver("ST", 11)
        elif payload == "---":
            self.setDriver("ST", 12)
        else:
            LOGGER.error("Invalid payload {}".format(payload))
            payload = "ERR"
            self.setDriver("ST", 4)

    def reset_send(self, command):
        self.controller.mqtt_pub(self.cmd_topic, "RESET")

    def query(self, command=None):
        self.controller.mqtt_pub(self.cmd_topic, "")
        self.reportDrivers()

    drivers = [{"driver": "ST", "value": 0, "uom": 25}]

    id = "MQFLAG"

    commands = {"QUERY": query, "RESET": reset_send}


# This class is an attempt to add support for temperature/humidity sensors.
# It was originally developed with a DHT22, but should work with
# any of the following, since they I believe they get identified by tomaso the same:
# DHT21, AM2301, AM2302, AM2321
# Should be easy to add other temp/humdity sensors.
class MQdht(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.on = False

    def start(self):
        pass

    def updateInfo(self, payload):
        try:
            data = json.loads(payload)
        except Exception as ex:
            LOGGER.error(
                "Failed to parse MQTT Payload as Json: {} {}".format(ex, payload)
            )
            return False
        if "AM2301" in data:
            self.setDriver("ST", 1)
            self.setDriver("CLITEMP", data["AM2301"]["Temperature"])
            self.setDriver("CLIHUM", data["AM2301"]["Humidity"])
        else:
            self.setDriver("ST", 0)

    def query(self, command=None):
        self.reportDrivers()

    drivers = [
        {"driver": "ST", "value": 0, "uom": 2},
        {"driver": "CLITEMP", "value": 0, "uom": 17},
        {"driver": "CLIHUM", "value": 0, "uom": 22},
    ]

    id = "MQDHT"

    commands = {"QUERY": query}


# This class is an attempt to add support for temperature only sensors.
# was made for DS18B20 waterproof
class MQds(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.on = False

    def start(self):
        pass

    def updateInfo(self, payload):
        try:
            data = json.loads(payload)
        except Exception as ex:
            LOGGER.error(
                "Failed to parse MQTT Payload as Json: {} {}".format(ex, payload)
            )
            return False
        if "DS18B20" in data:
            self.setDriver("ST", 1)
            self.setDriver("CLITEMP", data["DS18B20"]["Temperature"])
        else:
            self.setDriver("ST", 0)

    def query(self, command=None):
        self.reportDrivers()

    drivers = [
        {"driver": "ST", "value": 0, "uom": 2},
        {"driver": "CLITEMP", "value": 0, "uom": 17},
    ]

    id = "MQDS"

    commands = {"QUERY": query}


# This class is an attempt to add support for temperature/humidity/pressure sensors.
# Currently supports the BME280.  Could be extended to accept others.
class MQbme(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.on = False

    def start(self):
        pass

    def updateInfo(self, payload):
        try:
            data = json.loads(payload)
        except Exception as ex:
            LOGGER.error(
                "Failed to parse MQTT Payload as Json: {} {}".format(ex, payload)
            )
            return False
        if "BME280" in data:
            self.setDriver("ST", 1)
            self.setDriver("CLITEMP", data["BME280"]["Temperature"])
            self.setDriver("CLIHUM", data["BME280"]["Humidity"])
            # Converting to "Hg, could do this in sonoff-tomasto
            # or just report the raw hPA (or convert to kPA).
            press = format(
                round(float(".02952998751") * float(data["BME280"]["Pressure"]), 2)
            )
            self.setDriver("BARPRES", press)
        else:
            self.setDriver("ST", 0)

    def query(self, command=None):
        self.reportDrivers()

    drivers = [
        {"driver": "ST", "value": 0, "uom": 2},
        {"driver": "CLITEMP", "value": 0, "uom": 17},
        {"driver": "CLIHUM", "value": 0, "uom": 22},
        {"driver": "BARPRES", "value": 0, "uom": 23},
    ]

    id = "MQBME"

    commands = {"QUERY": query}


# This class is an attempt to add support for HC-SR04 Ultrasonic Sensor.
# Returns distance in centimeters.
class MQhcsr(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.on = False

    def start(self):
        pass

    def updateInfo(self, payload):
        try:
            data = json.loads(payload)
        except Exception as ex:
            LOGGER.error(
                "Failed to parse MQTT Payload as Json: {} {}".format(ex, payload)
            )
            return False
        if "SR04" in data:
            self.setDriver("ST", 1)
            self.setDriver("DISTANC", data["SR04"]["Distance"])
        else:
            self.setDriver("ST", 0)
            self.setDriver("DISTANC", 0)

    def query(self, command=None):
        self.reportDrivers()

    drivers = [
        {"driver": "ST", "value": 0, "uom": 2},
        {"driver": "DISTANC", "value": 0, "uom": 5},
    ]

    id = "MQHCSR"

    commands = {"QUERY": query}


# General purpose Analog input using ADC.
# Setting max value in editor.xml as 1024, as that would be the max for
# onboard ADC, but that might need to be changed for external ADCs.
class MQAnalog(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.on = False

    def start(self):
        pass

    def updateInfo(self, payload):
        try:
            data = json.loads(payload)
        except Exception as ex:
            LOGGER.error(
                "Failed to parse MQTT Payload as Json: {} {}".format(ex, payload)
            )
            return False
        if "ANALOG" in data:
            self.setDriver("ST", 1)
            self.setDriver("GPV", data["ANALOG"]["A0"])
        else:
            self.setDriver("ST", 0)
            self.setDriver("GPV", 0)

    def query(self, command=None):
        self.reportDrivers()

    # GPV = "General Purpose Value"
    # UOM:56 = "The raw value reported by device"
    drivers = [
        {"driver": "ST", "value": 0, "uom": 2},
        {"driver": "GPV", "value": 0, "uom": 56},
    ]

    id = "MQANAL"

    commands = {"QUERY": query}


# Reading the telemetry data for a Sonoff S31 (use the switch for control)
class MQs31(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.on = False

    def start(self):
        pass

    def updateInfo(self, payload):
        try:
            data = json.loads(payload)
        except Exception as ex:
            LOGGER.error(
                "Failed to parse MQTT Payload as Json: {} {}".format(ex, payload)
            )
            return False
        if "ENERGY" in data:
            self.setDriver("ST", 1)
            self.setDriver("CC", data["ENERGY"]["Current"])
            self.setDriver("CPW", data["ENERGY"]["Power"])
            self.setDriver("CV", data["ENERGY"]["Voltage"])
            self.setDriver("PF", data["ENERGY"]["Factor"])
            self.setDriver("TPW", data["ENERGY"]["Total"])
        else:
            self.setDriver("ST", 0)

    def query(self, command=None):
        self.reportDrivers()

    drivers = [
        {"driver": "ST", "value": 0, "uom": 2},
        {"driver": "CC", "value": 0, "uom": 1},
        {"driver": "CPW", "value": 0, "uom": 73},
        {"driver": "CV", "value": 0, "uom": 72},
        {"driver": "PF", "value": 0, "uom": 53},
        {"driver": "TPW", "value": 0, "uom": 33},
    ]

    id = "MQS31"

    commands = {"QUERY": query}


class MQraw(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.cmd_topic = device["cmd_topic"]
        self.on = False

    def start(self):
        pass

    def updateInfo(self, payload):
        try:
            self.setDriver("ST", 1)
            self.setDriver("GV1", int(payload))
        except Exception as ex:
            LOGGER.error("Failed to parse MQTT Payload: {} {}".format(ex, payload))

    def query(self, command=None):
        self.reportDrivers()

    drivers = [
        {"driver": "ST", "value": 0, "uom": 2},
        {"driver": "GV1", "value": 0, "uom": 56},
    ]

    id = "MQR"
    commands = {"QUERY": query}


# Class for an RGBW strip powered through a microController running MQTT client
# able to set colours and run different transition programs
class MQRGBWstrip(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.cmd_topic = device["cmd_topic"]
        self.on = False
        self.motion = False

    def start(self):
        pass

    def updateInfo(self, payload):
        try:
            data = json.loads(payload)
        except Exception as ex:
            LOGGER.error(
                "Failed to parse MQTT Payload as Json: {} {}".format(ex, payload)
            )
            return False

        # LED
        if "state" in data:
            # LED is present
            if data["state"] == "ON":
                self.setDriver("GV0", 100)
            else:
                self.setDriver("GV0", 0)
            if "br" in data:
                self.setDriver("GV1", data["br"])
            if "c" in data:
                if "r" in data["c"]:
                    self.setDriver("GV2", data["c"]["r"])
                if "g" in data["c"]:
                    self.setDriver("GV3", data["c"]["g"])
                if "b" in data["c"]:
                    self.setDriver("GV4", data["c"]["b"])
                if "w" in data["c"]:
                    self.setDriver("GV5", data["c"]["w"])
            if "pgm" in data:
                self.setDriver("GV6", data["pgm"])

    def led_on(self, command):
        self.controller.mqtt_pub(self.cmd_topic, json.dumps({"state": "ON"}))

    def led_off(self, command):
        self.controller.mqtt_pub(self.cmd_topic, json.dumps({"state": "OFF"}))

    def rgbw_set(self, command):
        query = command.get("query")
        red = self._check_limit(int(query.get("STRIPR.uom100")))
        green = self._check_limit(int(query.get("STRIPG.uom100")))
        blue = self._check_limit(int(query.get("STRIPB.uom100")))
        white = self._check_limit(int(query.get("STRIPW.uom100")))
        brightness = self._check_limit(int(query.get("STRIPI.uom100")))
        program = self._check_limit(int(query.get("STRIPP.uom100")))
        cmd = {
            "state": "ON",
            "br": brightness,
            "c": {"r": red, "g": green, "b": blue, "w": white},
            "pgm": program,
        }

        self.controller.mqtt_pub(self.cmd_topic, json.dumps(cmd))

    def _check_limit(self, value):
        if value > 255:
            return 255
        elif value < 0:
            return 0
        else:
            return value

    def query(self, command=None):
        self.reportDrivers()

    drivers = [
        {"driver": "ST", "value": 0, "uom": 2},
        {"driver": "GV0", "value": 0, "uom": 78},
        {"driver": "GV1", "value": 0, "uom": 100},
        {"driver": "GV2", "value": 0, "uom": 100},
        {"driver": "GV3", "value": 0, "uom": 100},
        {"driver": "GV4", "value": 0, "uom": 100},
        {"driver": "GV5", "value": 0, "uom": 100},
        {"driver": "GV6", "value": 0, "uom": 100},
    ]

    id = "MQRGBW"

    commands = {"QUERY": query, "DON": led_on, "DOF": led_off, "SETRGBW": rgbw_set}


if __name__ == "__main__":
    try:
        polyglot = polyinterface.Interface("MQTT")
        polyglot.start()
        control = Controller(polyglot)
        control.runForever()
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
