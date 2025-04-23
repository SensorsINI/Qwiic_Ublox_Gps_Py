# Python library for the SparkFun's line of u-Blox GPS units.
#
# SparkFun GPS-RTK NEO-M8P:
#   https://www.sparkfun.com/products/15005
# SparkFun GPS-RTK2 ZED-F9P:
#   https://www.sparkfun.com/products/15136
# SparkFun GPS ZOE-M8Q:
#   https://www.sparkfun.com/products/15193
# SparkFun GPS SAM-M8Q:
#   https://www.sparkfun.com/products/15210
# SparkFun GPS-RTK Dead Reckoning Phat ZED-F9R:
#   https://www.sparkfun.com/products/16475
# SparkFun GPS-RTK Dead Reckoning ZED-F9R:  
#   https://www.sparkfun.com/products/16344
# SparkFun GPS Dead Reckoning NEO-M9N:
#   https://www.sparkfun.com/products/15712
#
#------------------------------------------------------------------------
# Written by SparkFun Electronics, July 2020
#
# Do you like this library? Help suphard_port SparkFun. Buy a board!
#==================================================================================
# GNU GPL License 3.0
# Copyright (c) 2020 SparkFun Electronics
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# ==================================================================================
#
# The following code is a dependent on work by daymople and the awesome parsing
# capabilities of ubxtranslator: https://github.com/dalymople/ubxtranslator.
#
# pylint: disable=line-too-long, bad-whitespace, invalid-name, too-many-public-methods
#

from __future__ import division
from __future__ import absolute_import
import struct
import serial
import spidev

import threading
import time

import collections

import sys
import traceback

from . import sfeSpiWrapper
from . import sparkfun_predefines as sp
from . import core

_DEFAULT_NAME = u"Qwiic GPS"
_AVAILABLE_I2C_ADDRESS = [0x42]

class UbloxGps(object):
    u"""
    UbloxGps

    Initialize the library with the given port.

    :param hard_port:   The port to use to communicate with the module, this
                        can be a serial or SPI port. If no port is given, then the library
                        assumes serial at a 38400 baud rate.

    :return:            The UbloxGps object.
    :rtype:             Object
    """
    MAX_NMEA_LINES = 50
    MAX_ERRORS = 5



    device_name = _DEFAULT_NAME
    available_addresses = _AVAILABLE_I2C_ADDRESS

    def __init__(self, hard_port = None):
        if hard_port is None:
            self.hard_port = serial.Serial(u"/dev/serial0/", 38400, timeout=1)
        elif type(hard_port) == spidev.SpiDev:
            sfeSpi = sfeSpiWrapper(hard_port)
            self.hard_port = sfeSpi
        else:
            self.hard_port = hard_port

        self.nmea_line_buffer = collections.deque(maxlen=UbloxGps.MAX_NMEA_LINES)

        self.worker_exception_buffer = collections.deque(maxlen=UbloxGps.MAX_ERRORS)
        self.pckt_scl = {
            u'lon' : (10**-7),
            u'lat' :  (10**-7),
            u'headMot' :  (10**-5),
            u'headAcc' :  (10**-5),

            u'pDOP' :  0.01,
            u'gDOP' : 0.01,
            u'tDOP' : 0.01,
            u'vDOP' : 0.01,
            u'hDOP' : 0.01,
            u'nDOP' : 0.01,
            u'eDOP' : 0.01,

            u'headVeh' : (10**-5),
            u'magDec' : (10**-2),
            u'magAcc' : (10**-2),

            u'lonHp' : (10**-9),
            u'latHp' : (10**-9),
            u'heightHp' : 0.1,
            u'hMSLHp' : 0.1,
            u'hAcc' : 0.1,
            u'vAcc' : 0.1,

            u'errEllipseOrient': (10**-2),

            u'ecefX' : 0.1,
            u'ecefY' : 0.1,
            u'ecefZ' : 0.1,
            u'pAcc' : 0.1,

            u'prRes' : 0.1,

            u'cAcc' : (10**-5),
            u'heading' : (10**-5),

            u'relPosHeading' : (10**-5),
            u'relPosHPN' : 0.1,
            u'relPosHPE' : 0.1,
            u'relPosHPD' : 0.1,
            u'relPosHPLength' : 0.1,
            u'accN' : 0.1,
            u'accE' : 0.1,
            u'accD' : 0.1,
            u'accLength' : 0.1,
            u'accPitch' : (10**-5),
            u'accHeading' : (10**-5),

            u'roll' : (10**-5),
            u'pitch' : (10**-5),
        }

        #packet storage
        self.packets = {}
        # Class message values
        self.cls_ms = {}
        #class message list for auto-update
        self.cls_ms_auto = {}

        tmp_all_cls = []

        for (k,v) in vars(sp).items():
            if isinstance(v, core.Cls):
                tmp_all_cls.append(v)
                self.packets[v.name] = {}
                self.cls_ms_auto[v.name] = []
                self.cls_ms[v.name] = (v.id_, {})

                for (mk, mv) in v._messages.items():
                    self.cls_ms[v.name][1][mv.name] = mk

        self.parse_tool = core.Parser(tmp_all_cls)

        self.stopping = False

        self.thread = threading.Thread(target=self.run_packet_reader, args=())
        self.thread.daemon = True
        self.thread.start()


    def set_packet(self, cls_name, msg_name, payload):
        u"""
        Creates a new packet with the given class and message name. 
        """
        if (payload is None):
            if msg_name in self.packets[cls_name]:
                del self.packets[cls_name][msg_name]
        else:
            self.packets[cls_name][msg_name] = payload

    def wait_packet(self, cls_name, msg_name, wait_time):
        u"""
        Parses messages for the given class and message as long as the given time is not 
        exceeeded.
        :return: ublox message
        :rtype: namedtuple
        """
        if wait_time < 0 or wait_time is None:
            wait_time = 0

        orig_tm = time.time() # not in 2.7 time.monotonic()

        while (msg_name not in self.packets[cls_name]):
            time.sleep(0.05)

            if ((time.time()  - orig_tm) >= wait_time / 1000.0 ): # was time.monotonic()
                break

        return self.packets[cls_name][msg_name] if msg_name in self.packets[cls_name] else None

    def run_packet_reader(self):
        c2 = str()

        while True:
            try:
                if (self.stopping):
                    break

                c2 = c2 + self.hard_port.read(1)
                c2 = c2[-len(core.Parser.PREFIX):] # keep just 2 bytes, dump the rest

                if (c2[-1:] == '$'):
                    nmea_data = core.Parser._read_until(self.hard_port, '\x0d\x0a')
                    try:
                        self.nmea_line_buffer.append(u'$' + nmea_data.decode(u'utf-8').rstrip(u' \r\n'))
                    except:
                        pass #we just ignore bad messages, we don't ignore communication issues though

                    c2 = ''
                elif (c2 == core.Parser.PREFIX):
                    cls_name, msg_name, payload = self.parse_tool.receive_from(self.hard_port, True, True)

                    if not(cls_name is None or msg_name is None or payload is None):
                        self.set_packet(cls_name, msg_name, payload)

                if (self.stopping):
                    break

                time.sleep(0.01)

            except: # catch *all* exceptions
                self.worker_exception_buffer.append(sys.exc_info())
                # print(sys.exc_info())

                if (self.stopping):
                    break

    def stop(self):
        self.stopping = True
        self.thread.join()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.stop()

    def send_message(self, cls_name, msg_name, ubx_payload = None):
        u"""
        Sends a ublox message to the ublox module.

        :param cls_name:   The ublox class with which to send or receive the
                            message to/from.
        :param msg_name:      The message name under the ublox class with which
                            to send or receive the message to/from.
        :param ubx_payload: The payload to send to the class/id specified. If
                            none is given than a "poll request" is
                            initiated.
        :return: True on completion
        :rtype: boolean
        """

        ubx_class_id = self.cls_ms[cls_name][0] #convert names to ids
        ubx_id = self.cls_ms[cls_name][1][msg_name]

        SYNC_CHAR1 = 0xB5
        SYNC_CHAR2 = 0x62

        if ubx_payload == '\x00':
            ubx_payload = None

        if ubx_payload is None:
            payload_length = 0
        elif isinstance(ubx_payload, list):
            ubx_payload = str(ubx_payload)
            payload_length = len(ubx_payload)
        elif not(isinstance(ubx_payload, str)):
            ubx_payload = str([ubx_payload])
            payload_length = len(ubx_payload)
        else:
            payload_length = len(ubx_payload)


        message = str((SYNC_CHAR1, SYNC_CHAR2,
                      ubx_class_id, ubx_id, (payload_length & 0xFF),
                      (payload_length >> 8)))

        if payload_length > 0:
             message = message + ubx_payload

        checksum = core.Parser._generate_fletcher_checksum(message[2:])

        self.hard_port.write(message + checksum)

        return True

    def request_standard_packet(self, cls_name, msg_name, ubx_payload = None, wait_time = 2500, rethrow_thread_exception = True):
        u"""
        Sends a poll request for the ubx_class_id class with the ubx_id Message ID and
        parses ublox messages for the response. The payload is extracted from
        the response which is then scaled and passed to the user.
        :return: The payload of the ubx_class_id Class and ubx_id Message ID
        :rtype: namedtuple
        """

        if ubx_payload == '\x00':
            ubx_payload = None

        if not(msg_name in self.cls_ms_auto[cls_name] and ubx_payload is None):
            self.set_packet(cls_name, msg_name, None)
            self.send_message(cls_name, msg_name, ubx_payload)

        orig_packet = self.wait_packet(cls_name, msg_name, wait_time)

        if len(self.worker_exception_buffer) > 0:
            err = self.worker_exception_buffer.popleft()
            # raise err[1].with_traceback(err[2])
            raise err[1]

        return self.scale_packet(orig_packet) if (orig_packet is not None) else None

    def scale_packet(self, packet):
        u"""
        Scales the given packet to the unit specified in the interface manual.
        :return: ublox payload
        :rtype: packet
        """
        dict_packet = packet._asdict()
        isdirty = False
        for (k,v) in dict_packet.items():
            if k in self.pckt_scl:
                isdirty = True
                dict_packet[k] = self.pckt_scl[k] * v

        return type(packet)(**dict_packet) if isdirty else packet #we only need to reallocate and rebuild packet if it was changed


    def stream_nmea(self, wait_for_nmea = True):
        u"""
        Checks NMEA buffer for new messages and passes them back to the user 
        when detected. 
        :return: nmea message
        :rtype: string
        """
        while wait_for_nmea and len(self.nmea_line_buffer) == 0:
            time.sleep(0.05)

        return self.nmea_line_buffer.popleft() if len(self.nmea_line_buffer) > 0 else None

    def ubx_get_val(self, key_id, layer = 0, wait_time = 2500):
        u"""
        This function takes the given key id and breakes it into individual bytes
        which are then cocantenated together. This payload is then sent along
        with the CFG Class and VALGET Message ID to send_message(). Ublox
        Messages are then parsed for the requested values or a NAK signifying a
        problem.
        :return: The requested payload or a NAK on failure.
        :rtype: namedtuple
        """

        # layer 0 (RAM) and layer 7 (Default)! are the only options
        if layer != 7:
            layer = 0

        payloadCfg = [0,layer,0,0,
            (key_id) & 255,
            (key_id >> 8) & 255,
            (key_id >> 16) & 255,
            (key_id >> 24) & 255
        ]

        return self.request_standard_packet(u'CFG', u'VALGET', str(payloadCfg), wait_time = wait_time) #we return result immediately, hope get_val request won't overlap

    def ubx_set_val(self, key_id, ubx_payload, layer = 7, wait_time = 2500):
        u"""
        This function takes the given key id and breakes it into individual bytes
        which are then cocantenated together. This payload is then sent along
        with the CFG Class and VALSET Message ID to send_message(). Ublox
        Messages are then parsed for the requested values or a NAK signifying a
        problem.
        :return: None
        :rtype: namedtuple
        """


        if ubx_payload is None:
            return
        elif isinstance(ubx_payload, list):
            ubx_payload = str(ubx_payload)
        elif not(isinstance(ubx_payload, str)):
            ubx_payload = str([ubx_payload])

        if len(ubx_payload) == 0:
            return


        payloadCfg = [0,layer,0,0,
            (key_id) & 255,
            (key_id >> 8) & 255,
            (key_id >> 16) & 255,
            (key_id >> 24) & 255
        ]

        self.request_standard_packet(u'CFG', u'VALSET', str(payloadCfg) + ubx_payload, wait_time = wait_time)

    def set_auto_msg(self, cls_name, msg_name, freq, wait_time = 2500):
        ubx_class_id = self.cls_ms[cls_name][0] #convert names to ids
        ubx_id = self.cls_ms[cls_name][1][msg_name]

        if freq is None:
            freq = 0
        elif isinstance(freq, bool):
            freq = 1 if freq else 0
        elif freq < 0:
            freq = 0

        if msg_name in self.cls_ms_auto[cls_name]:
            self.cls_ms_auto[cls_name].remove(msg_name)

        if freq > 0:
            self.cls_ms_auto[cls_name].append(msg_name)

        payloadCfg = [ubx_class_id, ubx_id, freq]

        self.request_standard_packet(u'CFG', u'MSG', payloadCfg, wait_time = wait_time)

    def geo_coords(self, wait_time = 2500):
        u"""
-       Sends a poll request for NAV class and the PVT Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'NAV', u'PVT', wait_time = wait_time)

    def get_DOP(self, wait_time = 2500):
        u"""
-       Sends a poll request for NAV class and the DOP Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'NAV', u'DOP', wait_time = wait_time)

    def geo_cov(self, wait_time = 2500):
        u"""
-       Sends a poll request for NAV class and the COV Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'NAV', u'COV', wait_time = wait_time)

    def hp_geo_coords(self, wait_time = 2500):
        u"""
-       Sends a poll request for NAV class and the HPPOSLLH Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'NAV', u'HPPOSLLH', wait_time = wait_time)

    def date_time(self, wait_time = 2500):
        u"""
-       Sends a poll request for NAV class and the HPPOSLLH Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'NAV', u'PVT', wait_time = wait_time)

    def satellites(self, wait_time = 2500):
        u"""
-       Sends a poll request for NAV class and the SAT Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'NAV', u'SAT', wait_time = wait_time)

    def veh_attitude(self, wait_time = 2500):
        u"""
-       Sends a poll request for NAV class and the ATT Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'NAV', u'ATT', wait_time = wait_time)

    def imu_alignment(self, wait_time = 2500):
        u"""
-       Sends a poll request for ESF class and the ALG Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'ESF', u'ALG', wait_time = wait_time)

    def vehicle_dynamics(self, wait_time = 2500):
        u"""
-       Sends a poll request for ESF class and the INS Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'ESF', u'INS', wait_time = wait_time)

    def esf_measures(self, wait_time = 2500):
        u"""
-       Sends a poll request for ESF class and the MEAS Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'ESF', u'MEAS', wait_time = wait_time)

    def esf_raw_measures(self, wait_time = 2500):
        u"""
-       Sends a poll request for ESF class and the RAW Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'ESF', u'RAW', wait_time = wait_time)

    def reset_imu_align(self, wait_time = 2500):
        u"""
-       Sends a poll request for ESF class and the RESETALG Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'ESF', u'RESETALG', wait_time = wait_time)

    def esf_status(self, wait_time = 2500):
        u"""
-       Sends a poll request for ESF class and the RESETALG Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'ESF', u'STATUS', wait_time = wait_time)

    def port_settings(self, wait_time = 2500):
        u"""
-       Sends a poll request for ESF class and the COMMS Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'MON', u'COMMS', wait_time = wait_time)

    def module_gnss_support(self, wait_time = 2500):
        u"""
-       Sends a poll request for MON class and the GNSS Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'MON', u'GNSS', wait_time = wait_time)

    def pin_settings(self, wait_time = 2500):
        u"""
-       Sends a poll request for MON class and the HW2 Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'MON', u'HW3', wait_time = wait_time)

    def installed_patches(self, wait_time = 2500):
        u"""
-       Sends a poll request for MON class and the PATCH Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'MON', u'PATCH', wait_time = wait_time) #changed from HW3 to PATCH

    def prod_test_pio(self, wait_time = 2500):
        u"""
-       Sends a poll request for MON class and the PIO Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'MON', u'PIO', wait_time = wait_time)

    def prod_test_monitor(self, wait_time = 2500):
        u"""
-       Sends a poll request for MON class and the PT2 Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'MON', u'PT2', wait_time = wait_time)

    def rf_ant_status(self, wait_time = 2500):
        u"""
-       Sends a poll request for MON class and the RF Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'MON', u'RF', wait_time = wait_time)

    def module_wake_state(self, wait_time = 2500): #No response on F9P
        u"""
-       Sends a poll request for MON class and the RXR Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'MON', u'RXR', wait_time = wait_time)

    def sensor_production_test(self, wait_time = 2500):#No response on F9P
        u"""
-       Sends a poll request for MON class and the SPT Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'MON', u'SPT', wait_time = wait_time)

    def module_software_version(self, wait_time = 2500):
        u"""
-       Sends a poll request for MON class and the VER Message 
+       The response is then passed on to the user.

        :return: ublox payload
        :rtype: namedtuple
        """
        return self.request_standard_packet(u'MON', u'VER', wait_time = wait_time)
