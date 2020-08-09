import logging
import multiprocessing
import time

import paho.mqtt.client as mqtt
#import paho.mqtt.publish as publish


class MQTTClient(multiprocessing.Process):
    def __init__(self, messageQ, commandQ, config):
        self.logger = logging.getLogger('RFLinkGW.MQTTClient')
        self.logger.info("Starting...")

        multiprocessing.Process.__init__(self)
        self.messageQ = messageQ
        self.commandQ = commandQ

        self.config = config
        self.auth = None
        self.host = config['mqtt_host']
        self.port = config['mqtt_port']

        self.mqtt_data_prefix = config['mqtt_prefix']
        self._mqttConn = mqtt.Client(client_id='RFLinkGateway')
        self._mqttConn.will_set("clients/RFLinkGateway", "0", 0, True)
        if 'mqtt_user' in config and config['mqtt_user'] is not None:
            self.logger.info("Connection with credentials (user: %s).", config['mqtt_user'])
            self._mqttConn.username_pw_set(username=config['mqtt_user'], password=config['mqtt_password'])
            self.auth = {'username': config['mqtt_user'], 'password': config['mqtt_password']}
        self._mqttConn.connect(config['mqtt_host'], port=config['mqtt_port'], keepalive=120)

        self._mqttConn.on_connect = self._on_connect
        self._mqttConn.on_disconnect = self._on_disconnect
        self._mqttConn.on_publish = self._on_publish
        self._mqttConn.on_message = self._on_message

    def close(self):
        self.logger.info("Closing connection")
        self._mqttConn.disconnect()

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("Connected to broker. Return code: %s" % mqtt.connack_string(rc))
            self._mqttConn.publish("clients/RFLinkGateway", "1", 0, True)
            self._mqttConn.subscribe([ ("%s/+/+/W/+" % self.mqtt_data_prefix, 2), ("%s/_COMMAND/IN" % self.mqtt_data_prefix, 2) ])
        else:
            self.logger.warning("An error occured on connect. Return code: %s " % mqtt.connack_string(rc))

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            self.logger.error("Unexpected disconnection. Return code: %s" % mqtt.connack_string(rc))

    def _on_publish(self, client, userdata, mid):
        self.logger.debug("Message " + str(mid) + " published.")

    def _on_message(self, client, userdata, message):
        if message.topic == (self.mqtt_data_prefix + "/_COMMAND/IN"):
            payload = message.payload.decode('ascii')
            self.logger.debug('Special Control Command received: %s' % (payload))
            data_out = {
                'action': 'SCC',
                'topic': message.topic,
                'family': '',
                'device_id': '',
                'param': '',
                'payload': payload,
                'qos': 1
             }
        else:
            self.logger.debug("Message received on topic: %s" % (message.topic))
            data = message.topic.replace(self.mqtt_data_prefix + "/", "").split("/")
            data_out = {
                'action': 'NCC',
                'topic': message.topic,
                'family': data[0],
                'device_id': data[1],
                'param': data[3],
                'payload': message.payload.decode('ascii'),
                'qos': 1
            }
        self.commandQ.put(data_out)

    def publish(self, task):
        if len(task['family']) > 0:
            subtopic = "%s/%s/R/%s" % (task['family'], task['device_id'], task['param'])
        else:
            subtopic = "_COMMAND/OUT"
        topic = "%s/%s" % (self.mqtt_data_prefix, subtopic)

        try:
            self.logger.debug('Sending:%s to %s' % (task, topic))
            self._mqttConn.publish(topic, payload=task['payload'])
            #publish.single(topic, payload=task['payload'], hostname=self.host, auth=self.auth, port=self.port)
        except Exception as e:
            self.logger.error('Publish problem: %s' % (e))
            self.messageQ.put(task)

    def run(self):
        while True:
            if not self.messageQ.empty():
                task = self.messageQ.get()
                self.publish(task)
            else:
                time.sleep(0.01)
            self._mqttConn.loop()
