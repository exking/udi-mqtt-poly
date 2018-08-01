#!/usr/bin/env python3

import polyinterface
import sys
import logging
import paho.mqtt.client as mqtt
import json

LOGGER = polyinterface.LOGGER

class Controller(polyinterface.Controller):
    def __init__(self, polyglot):
        super().__init__(polyglot)
        self.name = 'MQTT Controller'
        self.address = 'mqctrl'
        self.primary = self.address
        self.mqtt_server = 'localhost'
        self.mqtt_port = 1883
        self.mqtt_user = None
        self.mqtt_password = None
        self.devlist = None
        # example: [ {'id': 'sonoff1', 'type': 'switch', 'status_topic': 'stat/sonoff1/power', 'cmd_topic': 'cmnd/sonoff1/power'} ]
        self.status_topics = []
        self.mqttc = None

    def start(self):
        # LOGGER.setLevel(logging.INFO)
        LOGGER.info('Started MQTT controller')
        if 'mqtt_server' in self.polyConfig['customParams']:
            self.mqtt_server = self.polyConfig['customParams']['mqtt_server']
        if 'mqtt_port' in self.polyConfig['customParams']:
            self.mqtt_port = int(self.polyConfig['customParams']['mqtt_port'])
        if 'mqtt_user' not in self.polyConfig['customParams']:
            LOGGER.error('mqtt_user must be configured')
            return False
        if 'mqtt_password' not in self.polyConfig['customParams']:
            LOGGER.error('mqtt_password must be configured')
            return False
        if 'devlist' not in self.polyConfig['customParams']:
            LOGGER.error('devlist must be configured')
            return False

        self.mqtt_user = self.polyConfig['customParams']['mqtt_user']
        self.mqtt_password = self.polyConfig['customParams']['mqtt_password']
        try:
            self.devlist = json.loads(self.polyConfig['customParams']['devlist'])
        except Exception as ex:
            LOGGER.error('Failed to parse the devlist: {}'.format(ex))
            return False

        self.mqttc = mqtt.Client()
        self.mqttc.on_connect = self._on_connect
        self.mqttc.on_disconnect = self._on_disconnect
        self.mqttc.on_message = self._on_message
        self.mqttc.is_connected = False

        for dev in self.devlist:
            if 'id' not in dev or 'status_topic' not in dev or 'cmd_topic' not in dev or 'type' not in dev:
                LOGGER.error('Invalid device definition: {}'.format(json.dumps(dev)))
                continue
            name = dev['id']
            address = name.lower()[:14]
            if dev['type'] == 'switch':
                if not address is self.nodes:
                    LOGGER.info('Adding {} {}'.format(dev['type'], name))
                    self.addNode(MQSwitch(self, self.address, address, name, dev))
                    self.status_topics.append(dev['status_topic'])
            elif dev['type'] == 'sensor':
                if not address is self.nodes:
                    LOGGER.info('Adding {} {}'.format(dev['type'], name))
                    self.addNode(MQSensor(self, self.address, address, name, dev))
                    self.status_topics.append(dev['status_topic'])
            else:
                LOGGER.error('Device type {} is not yet supported'.format(dev['type']))
        LOGGER.info('Done adding nodes, connecting to MQTT broker...')
        self.mqttc.username_pw_set(self.mqtt_user, self.mqtt_password)
        try:
            self.mqttc.connect(self.mqtt_server, self.mqtt_port, 10)
            self.mqttc.loop_start()
        except Exception as ex:
            LOGGER.error('Error connecting to Poly MQTT broker {}'.format(ex))
            return False

        return True

    def _on_connect(self, mqttc, userdata, flags, rc):
        if rc == 0:
            LOGGER.info('Poly MQTT Connected, subscribing...')
            self.mqttc.is_connected = True
            results = []
            for stopic in self.status_topics:
                results.append((stopic, tuple(self.mqttc.subscribe(stopic))))
            for (topic, (result, mid)) in results:
                if result == 0:
                    LOGGER.info('Subscribed to {} MID: {}, res: {}'.format(topic, mid, result))
                else:
                    LOGGER.error('Failed to subscribe {} MID: {}, res: {}'.format(topic, mid, result))
            for node in self.nodes:
                if self.nodes[node].address != self.address:
                    self.nodes[node].query()
        else:
            LOGGER.error('Poly MQTT Connect failed')

    def _on_disconnect(self, mqttc, userdata, rc):
        self.mqttc.is_connected = False
        if rc != 0:
            LOGGER.warning('Poly MQTT disconnected, trying to re-connect')
            try:
                self.mqttc.reconnect()
            except Exception as ex:
                LOGGER.error('Error connecting to Poly MQTT broker {}'.format(ex))
                return False
        else:
            LOGGER.info('Poly MQTT graceful disconnection')

    def _on_message(self, mqttc, userdata, message):
        topic = message.topic
        payload = message.payload.decode('utf-8')
        LOGGER.debug('Received {} from {}'.format(payload, topic))
        try:
            self.nodes[self._dev_by_topic(topic)].updateInfo(payload)
        except Exception as ex:
            LOGGER.error('Failed to process message {}'.format(ex))

    def _dev_by_topic(self, topic):
        for dev in self.devlist:
            if dev['status_topic'] == topic:
                return dev['id'].lower()[:14]
        return None

    def mqtt_pub(self, topic, message):
        self.mqttc.publish(topic, message, retain=False)

    def stop(self):
        self.mqttc.loop_stop()
        self.mqttc.disconnect()
        LOGGER.info('MQTT is stopping')

    def updateInfo(self):
        pass

    def query(self, command=None):
        for node in self.nodes:
            self.nodes[node].reportDrivers()

    def discover(self):
        pass

    id = 'MQCTRL'
    commands = {'DISCOVER': discover}
    drivers = [{'driver': 'ST', 'value': 0, 'uom': 2}]


class MQSwitch(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.cmd_topic = device['cmd_topic']
        self.on = False

    def start(self):
        pass

    def updateInfo(self, payload):
        if payload == 'ON':
            if not self.on:
                self.reportCmd('DON')
                self.on = True
            self.setDriver('ST', 100)
        elif payload == 'OFF':
            if self.on:
                self.reportCmd('DOF')
                self.on = False
            self.setDriver('ST', 0)
        else:
            LOGGER.error('Invalid payload {}'.format(payload))

    def set_on(self, command):
        self.on = True
        self.controller.mqtt_pub(self.cmd_topic, 'ON')

    def set_off(self, command):
        self.on = False
        self.controller.mqtt_pub(self.cmd_topic, 'OFF')

    def query(self, command=None):
        self.controller.mqtt_pub(self.cmd_topic, '')
        self.reportDrivers()

    drivers = [{'driver': 'ST', 'value': 0, 'uom': 78}
              ]

    id = 'MQSW'

    commands = {
            'QUERY': query, 'DON': set_on, 'DOF': set_off
               }


class MQSensor(polyinterface.Node):
    def __init__(self, controller, primary, address, name, device):
        super().__init__(controller, primary, address, name)
        self.cmd_topic = device['cmd_topic']
        self.on = False
        self.motion = False

    def start(self):
        pass

    def updateInfo(self, payload):
        try:
            data = json.loads(payload)
        except Exception as ex:
            LOGGER.error('Failed to parse MQTT Payload as Json: {} {}'.format(ex, payload))
            return False

        # motion detector
        if 'motion' in data:
            if data['motion'] == 'standby':
                self.setDriver('ST', 0)
                if self.motion:
                    self.motion = False
                    self.reportCmd('DOF')
            else:
                self.setDriver('ST', 1)
                if not self.motion:
                    self.motion = True
                    self.reportCmd('DON')
        else:
            self.setDriver('ST', 0)
        # temperature
        if 'temperature' in data:
            self.setDriver('CLITEMP', data['temperature'])
        # heatIndex
        if 'heatIndex' in data:
            self.setDriver('GPV', data['heatIndex'])
        # humidity
        if 'humidity' in data:
            self.setDriver('CLIHUM', data['humidity'])
        # light detecor reading
        if 'ldr' in data:
            self.setDriver('LUMIN', data['ldr'])
        # LED
        if 'state' in data:
            # LED is present
            if data['state'] == 'ON':
                self.setDriver('GV0', 100)
            else:
                self.setDriver('GV0', 0)
            if 'brightness' in data:
                self.setDriver('GV1', data['brightness'])
            if 'color' in data:
                if 'r' in data['color']:
                    self.setDriver('GV2', data['color']['r'])
                if 'g' in data['color']:
                    self.setDriver('GV3', data['color']['g'])
                if 'b' in data['color']:
                    self.setDriver('GV4', data['color']['b'])


    def led_on(self, command):
        self.controller.mqtt_pub(self.cmd_topic, json.dumps({'state': 'ON'}))

    def led_off(self, command):
        self.controller.mqtt_pub(self.cmd_topic, json.dumps({'state': 'OFF'}))

    def led_set(self, command):
        query = command.get('query')
        red = self._check_limit(int(query.get('R.uom100')))
        green = self._check_limit(int(query.get('G.uom100')))
        blue = self._check_limit(int(query.get('B.uom100')))
        brightness = self._check_limit(int(query.get('I.uom100')))
        transition = int(query.get('D.uom58'))
        flash = int(query.get('F.uom58'))
        cmd = { 'state': 'ON', 'brightness': brightness, 'color': {'r': red, 'g': green, 'b': blue } }
        if transition > 0:
            cmd['transition'] = transition
        if flash > 0:
            cmd['flash'] = flash

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

    drivers = [{'driver': 'ST', 'value': 0, 'uom': 2},
               {'driver': 'CLITEMP', 'value': 0, 'uom': 17},
               {'driver': 'GPV', 'value': 0, 'uom': 17},
               {'driver': 'CLIHUM', 'value': 0, 'uom': 22},
               {'driver': 'LUMIN', 'value': 0, 'uom': 36},
               {'driver': 'GV0', 'value': 0, 'uom': 78},
               {'driver': 'GV1', 'value': 0, 'uom': 100},
               {'driver': 'GV2', 'value': 0, 'uom': 100},
               {'driver': 'GV3', 'value': 0, 'uom': 100},
               {'driver': 'GV4', 'value': 0, 'uom': 100}
              ]

    id = 'MQSENS'

    commands = {
            'QUERY': query, 'DON': led_on, 'DOF': led_off, 'SETLED': led_set
               }


if __name__ == "__main__":
    try:
        polyglot = polyinterface.Interface('MQTT')
        polyglot.start()
        control = Controller(polyglot)
        control.runForever()
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)
