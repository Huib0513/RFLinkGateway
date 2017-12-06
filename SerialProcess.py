import logging
import multiprocessing
import time

import serial

class SerialProcess(multiprocessing.Process):
    def __init__(self, messageQ, commandQ, config):
        self.logger = logging.getLogger('RFLinkGW.SerialProcessing')

        self.logger.info("Starting...")
        multiprocessing.Process.__init__(self)

        self.messageQ = messageQ
        self.commandQ = commandQ

        self.gatewayPort = config['rflink_tty_device']
        self.sp = serial.Serial()
        self.connect()

        self.processing_exception = config['rflink_direct_output_params']
        self.ignored_devices = config['rflink_ignored_devices']
        self.logger.info("Ignoring devices: %s", self.ignored_devices)

    def signedhex2dec(self, value):
        val = int(value, 16)
        if val >= 0x8000: val = -1*(val - 0x8000)
        return val

    def close(self):
        self.sp.close()
        self.logger.debug('Serial closed')

    def prepare_output(self, data_in):
        out = []
        msg = data_in.decode("ascii")
        data = msg.replace(";\r\n", "").split(";")

        if len(data) > 1 and data[1] == '00':
            self.logger.info("%s" % (data[2]))
        else:
            self.logger.debug("Received message:%s" % (data))

            if len(data) > 3 and data[0] == '20' and data[2].split("=")[0] != 'VER' : # Special Control Command 'VERSION' returns a len=5 data object. This trick is necessary... but not very clean
                family = data[2]
                deviceId = data[3].split("=")[1]  # TODO: For some debug messages there is no =
                if (deviceId not in self.ignored_devices and
                    family not in self.ignored_devices and
                    "%s/%s" % (family, deviceId) not in self.ignored_devices):
                    d = {'message': msg}
                    for t in data[4:]:
                        token = t.split("=")
                        d[token[0]] = token[1]
                    for key in d:
                        if key in self.processing_exception:
                            val = d[key]
                        else:
                            val = self.signedhex2dec(d[key]) / 10.0
                        data_out = {
                            'action': 'NCC',
                            'topic': '',
                            'family': family,
                            'deviceId': deviceId,
                            'param': key,
                            'payload': str(val),
                            'qos': 1,
                            'timestamp': time.time()
                        }
                        out = out + [data_out]
            elif (len(data) == 3 and data[0] == '20') or (len(data) > 3 and data[0] == '20' and data[2].split("=")[0] == 'VER'):
                payload = ";".join(data[2:])
                data_out = {
                            'action': 'SCC',
                            'topic': '',
                            'family': '',
                            'deviceId': '',
                            'param': '',
                            'payload': payload,
                            'qos': 1,
                            'timestamp': time.time()
                }
                out = [data_out]
        return out

    def prepare_input(self, task):
        if task['action'] == 'SCC':
            out_str = '10;%s;\n' % task['payload']
        else:
            out_str = '10;%s;%s;%s;%s;\n' % (task['family'], task['deviceId'], task['param'], task['payload'])
        self.logger.debug('Sending to serial:%s' % (out_str))
        return out_str

    def connect(self):
        self.logger.info('Connecting to serial')
        while not self.sp.isOpen():
            try:
                self.sp = serial.Serial(self.gatewayPort, 57600, timeout=1)
                self.logger.debug('Serial connected')
            except Exception as e:
                self.logger.error('Serial port is closed %s' % (e))

    def run(self):
        self.sp.flushInput()
        while True:
            try:
                if not self.commandQ.empty():
                    task = self.commandQ.get()
                    # send it to the serial device if not in the devices ignored list
                    if task['deviceId'] not in self.ignored_devices:
                        self.sp.write(self.prepare_input(task).encode('ascii'))
                    else:
                        self.logger.debug('Nothing sent to serial: deviceId (%s) is in the devices ignored list.' % (task['deviceId']))
            except Exception as e:
                self.logger.error("Send error:%s" % (format(e)))
            try:
                if (self.sp.inWaiting() > 0):
                    data = self.sp.readline()
                    task_list = self.prepare_output(data)
                    for task in task_list:
                        self.logger.debug("Sending to Q:%s" % (task))
                        self.messageQ.put(task)
                else:
                    time.sleep(0.01)
            except Exception as e:
                self.logger.error('Receive error: %s' % (e))
                self.connect()
