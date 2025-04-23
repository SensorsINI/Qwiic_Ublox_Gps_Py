#This code is based on the work by daylomople (https://github.com/dalymople) and the awesome parsing capabilities of ubxtranslator (https://github.com/dalymople/ubxtranslator). 
u"""The core structure definitions"""

from __future__ import absolute_import
import struct
from collections import namedtuple
#from typing import List, Iterator, Union

__all__ = [u'PadByte', u'Field', u'Flag', u'BitField', u'RepeatedBlock', u'Message', u'Cls', u'Parser', ]


class PadByte(object):
    u"""A padding byte, used for padding the messages.

    The number of repeats needs to be used carefully...
    If you want 6 total pad bytes, the pad byte is repeated 5 times.

    If this proves confusing it may be changed in the future.
    """
    __slots__ = [u'repeat', ]

    def __init__(self, repeat = 0):
        self.repeat = repeat

    @property
    def repeated_block(self):
        return False

    @property
    def fmt(self):
        u"""Return the format char for use with the struct package"""
        return u'x' * (self.repeat + 1)

    @staticmethod
    def parse(_it):
        u"""Discard the padding bytes"""
        return None, None


class Field(object):
    u"""A field type that is used to describe most `normal` fields.

    The descriptor code is as per the uBlox data sheet available;
    https://www.u-blox.com/sites/default/files/products/documents/u-blox8-M8_ReceiverDescrProtSpec_%28UBX-13003221%29_Public.pdf

    Field types that are variable length are not supported at this stage.

    In future that support may be added but it would probably use a different field constructor...
    """
    __types__ = {u'U1': u'B', u'I1': u'b',
                 u'U2': u'H', u'I2': u'h',
                 u'U4': u'I', u'I4': u'i', u'R4': u'f',
                 u'R8': u'd', u'C': u'c', u'S': u's'}
    __slots__ = [u'name', u'_type', u'_len']

    def __init__(self, name, type_, len_ = 1):
        self.name = name

        if (len_ is None or len_ < 0):
            ValueError(u'The provided _len is not valid')


        self._len = len_

        if type_ not in Field.__types__:
            raise ValueError(u'The provided _type of {} is not valid'.format(type_))
        self._type = type_

    @property
    def repeated_block(self):
        return False

    @property
    def fmt(self):
        u"""Return the format char for use with the struct package"""

        return (unicode(self._len) if self._len > 1 else u'') + Field.__types__[self._type]

    def parse(self, it):
        u"""Return a tuple representing the provided value/s"""
        resp = []
        len = 1 if self._type == u'S' else self._len

        for i in xrange(0, len):
            resp.append(it.next())

        if self._type in [u'U1', u'I1', u'U2', u'I2', u'U4', u'I4', ]:
            for i in xrange(0, len):
                resp[i] = int(resp[i])
        elif self._type == u'R4':
            for i in xrange(0, len):
                resp[i] = float(resp[i])
        elif self._type == u'R8':
            for i in xrange(0, len):
                resp[i] = double(resp[i])
        elif self._type == u'S':
            resp = [resp[0].decode(u'ascii').rstrip(u'\0')]

        return self.name, resp[0] if len == 1 else resp


class Flag(object):
    u"""A flag within a bit field.

    The start and stop indexes are used in a similar way to list indexing.
    They are zero indexed and the stop is exclusive.

    So for flag at bit zero that is one bit wide the constructor would be;
    Flag('your flag name', 0, 1)

    This class does a basic check against the implied field width eg. < 4*8, but any
    strict checking is done within classes that use this. For example you can set a
    start and stop > 8 even if the bit field is only 8 bits wide.
    """
    __slots__ = [u'name', u'_start', u'_stop', u'_mask', ]

    def __init__(self, name, start, stop):
        self.name = name

        if 0 > start:
            raise ValueError(u'The start index must be greater than 0 not {}'.format(start))

        if start > stop:
            raise ValueError(u'The start index, {}, must be higher than the stop index, {}'.format(start, stop))

        if stop > 4 * 8:
            raise ValueError(u'The stop index must be less than 4 bytes wide not {}'.format(stop))

        self._start = start
        self._stop = stop

        self._mask = 0x00
        for i in xrange(start, stop):
            self._mask |= 0x01 << i

    def parse(self, value):
        u"""Return a tuple representing the provided value"""
        return self.name, (value & self._mask) >> self._start


class BitField(object):
    u"""A bit field type made up of flags.

    The bit field uses the types described within the uBlox data sheet:
    https://www.u-blox.com/sites/default/files/products/documents/u-blox8-M8_ReceiverDescrProtSpec_%28UBX-13003221%29_Public.pdf

    Flags should be passed within the constructor and should not be added after
    the class has been created. The constructor does check whether the flag field
    indexes would imply that the BitField is wider than specified. If this is found
    it will raise a ValueError.

    """

    __slots__ = [u'name', u'_type', u'_subfields', u'_nt', ]
    __types__ = {u'X1': u'B', u'X2': u'H', u'X4': u'I'}

    # noinspection PyProtectedMember
    def __init__(self, name, type_, subfields):
        self.name = name

        if type_ not in BitField.__types__:
            raise ValueError(u'The provided _type of {} is not valid'.format(type_))
        self._type = type_

        self._subfields = subfields

        if type_ == u'X1':
            width = 1
        elif type_ == u'X2':
            width = 2
        else:
            width = 4

        for sf in subfields:
            if sf._stop > (width * 8):
                raise ValueError(u'{} stop index of {} is wider than the implicit width of {} bytes'.format(
                    sf.__class__.__name__, sf._stop, width
                ))

        self._nt = namedtuple(self.name, [f.name for f in self._subfields])

    @property
    def repeated_block(self):
        return False

    @property
    def fmt(self):
        u"""Return the format char for use with the struct package"""
        return BitField.__types__[self._type]

    def parse(self, it):
        u"""Return a named tuple representing the provided value"""
        value = it.next()
        return self.name, self._nt(**dict((k, v) for k, v in [x.parse(value) for x in self._subfields]))


class RepeatedBlock(object):
    u"""Defines a repeated block of Fields within a UBX Message

    """
    __slots__ = [u'name', u'_fields', u'repeat', u'_nt', ]

    def __init__(self, name, fields):
        self.name = name
        self._fields = fields
        self.repeat = 0
        self._nt = namedtuple(self.name, [f.name for f in self._fields if hasattr(f, u'name')])

    @property
    def repeated_block(self):
        return True

    @property
    def fmt(self):
        u"""Return the format string for use with the struct package."""
        return u''.join([field.fmt for field in self._fields]) * (self.repeat + 1)

    def parse(self, it):
        u"""Return a tuple representing the provided value/s"""
        resp = []
        for i in xrange(self.repeat + 1):
            resp.append(self._nt(**dict((k, v) for k, v in [f.parse(it) for f in self._fields] if k is not None)))

        return self.name, resp


class Message(object):
    u"""Defines a UBX message.

    The Messages are described in the data sheet:
    https://www.u-blox.com/sites/default/files/products/documents/u-blox8-M8_ReceiverDescrProtSpec_%28UBX-13003221%29_Public.pdf

    The supplied name should be upper case. eg. PVT

    The fields that make up message should be passed into the constructor as a list with the fields
    in the correct order. Modifying the list or the fields after construction is not supported as it
    has side effects.

    The id is only allowed to be one byte wide so 0x00 <= id_ <= 0xFF values outside this range
    will raise a ValueError

    """
    __slots__ = [u'_id', u'name', u'_fields', u'_nt', u'_repeated_block', ]

    def __init__(self, id_, name, fields):
        if id_ < 0:
            raise ValueError(u'The _id must be >= 0, not {}'.format(id_))

        if id_ > 0xFF:
            raise ValueError(u'The _id must be <= 0xFF, not {}'.format(id_))

        self._id = id_
        self.name = name
        self._fields = fields
        self._nt = namedtuple(self.name, [f.name for f in self._fields if hasattr(f, u'name')])
        self._repeated_block = None

        for field in fields:
            if field.repeated_block:
                if self._repeated_block is not None:
                    raise ValueError(u'Cannot assign multiple repeated blocks to a message.')
                self._repeated_block = field

    @property
    def id_(self):
        u"""Public read only access to the message id"""
        return self._id

    @property
    def fmt(self):
        u"""Return the format string for use with the struct package."""
        return u''.join([field.fmt for field in self._fields])

    def parse(self, payload):
        u"""Return a named tuple parsed from the provided payload.

        If the provided payload is not the same length as what is implied by the format string
        then a ValueError is raised.
        """

        payload_len = len(payload)

        try:
            self._repeated_block.repeat = 0
        except AttributeError:
            pass

        while True:
            fmt_len = struct.calcsize(self.fmt)

            if fmt_len == payload_len:
                break

            if fmt_len > payload_len:
                raise ValueError(u'The payload length does not match the length implied by the message fields. ' +
                                 u'Expected {} actual {}'.format(struct.calcsize(self.fmt), len(payload)))

            try:
                self._repeated_block.repeat += 1
            except AttributeError:
                raise ValueError(u'The payload length does not match the length implied by the message fields. ' +
                                 u'Expected {} actual {}'.format(struct.calcsize(self.fmt), len(payload)))

        it = iter(struct.unpack(self.fmt, payload))

        return self.name, self._nt(**dict((k, v) for k, v in [f.parse(it) for f in self._fields] if k is not None))


class Cls(object):
    u"""Defines a UBX message class.

    The Classes are described in the data sheet:
    https://www.u-blox.com/sites/default/files/products/documents/u-blox8-M8_ReceiverDescrProtSpec_%28UBX-13003221%29_Public.pdf

    The messages within the class can be provided via the constructor or via the `register_msg` method.

    The id_ should not be modified after registering the class with the parser.
    """
    __slots__ = [u'_id', u'name', u'_messages', ]

    def __init__(self, id_, name, messages):
        if id_ < 0:
            raise ValueError(u'The _id must be >= 0, not {}'.format(id_))

        if id_ > 0xFF:
            raise ValueError(u'The _id must be <= 0xFF, not {}'.format(id_))

        self._id = id_

        self.name = name

        self._messages = {}
        for msg in messages:
            self._messages[msg.id_] = msg

    @property
    def id_(self):
        u"""Public read only access to the class id"""
        return self._id

    def __contains__(self, item):
        return self._messages.__contains__(item)

    def __getitem__(self, item):
        try:
            return self._messages[item]
        except KeyError:
            raise KeyError(u"A message of id {} has not been registered within {!r}".format(
                item, self
            ))

    def register_msg(self, msg):
        u"""Register a message type."""
        # noinspection PyProtectedMember
        self._messages[msg._id] = msg

    def parse(self, msg_id, payload):
        u"""Return a named tuple parsed from the provided payload.

        If the provided payload is not the same length as what is implied by the format string
        then a ValueError is raised.

        """
        name, nt = self._messages[msg_id].parse(payload)
        return self.name, name, nt


class Parser(object):
    u"""A lightweight UBX message parser.

    This class is designed to contain a set of message classes, when a stream is passed via the `receive_from`
    method the stream is read until the PREFIX is found, the message type is matched to the registered class
    and message id's, the length read, checksum checked and finally the message is deconstructed and returned
    as a named tuple.

    This is powered by the inbuilt struct package for the heavy lifting of the message decoding and there are
    no external dependencies.

    Message classes can be passed via the constructor or the `register_cls` method.

    The `receive_from` method doesn't care about the underlying type of the provided stream, so long as it
    supports a `read` method and returns bytes. It is up to the implementation to determine how to get the
    UBX packets from the device. The typical way to do this would be a serial package like pyserial, see the
    examples file.
    """
    PREFIX = str((0xB5, 0x62))

    def __init__(self, classes):
        self._input_buffer = ''

        self.classes = {}
        for cls in classes:
            self.classes[cls._id] = cls

    def register_cls(self, cls):
        u"""Register a message  class."""
        self.classes[cls.id_] = cls


    def receive_from(self, stream, skippreamble = False, ignoreunsupported = False):
        u"""Receive a message from a stream and return as a namedtuple.
        raise IOError or ValueError on errors.
        """
        while not(skippreamble):
            # Search for the prefix
            buff = self._read_until(stream, terminator=self.PREFIX)
            if buff[-2:] == self.PREFIX:
                break

        # read the first four bytes
        buff = stream.read(4)


        if len(buff) != 4:
            raise IOError(u"A stream read returned {} bytes, expected 4 bytes".format(len(buff)))

        # convert them into the packet descriptors
        msg_cls, msg_id, length = struct.unpack(u'BBH', buff)

        # check the packet validity
        if msg_cls not in self.classes:
            if ignoreunsupported:
                return (None, None, None)
            else:
                raise ValueError(u"Received message id of {:x} in unsupported class {:x}".format(msg_id, msg_cls))

        if msg_id not in self.classes[msg_cls]:
            if ignoreunsupported:
                return (None, None, None)
            else:
                raise ValueError(u"Received unsupported message id of {:x} in class {:x}".format(msg_id, msg_cls))

        # Read the payload
        buff += stream.read(length)
        if len(buff) != (4 + length):
            raise IOError(u"A stream read returned {} bytes, expected {} bytes".format(
                len(buff), 4 + length))

        # Read the checksum
        checksum_sup = stream.read(2)
        if len(checksum_sup) != 2:
            raise IOError(u"A stream read returned {} bytes, expected 2 bytes".format(len(buff)))

        checksum_cal = self._generate_fletcher_checksum(buff)
        if checksum_cal != checksum_sup:
            raise ValueError(u"Checksum mismatch. Calculated {:x} {:x}, received {:x} {:x}".format(
                checksum_cal[0], checksum_cal[1], checksum_sup[0], checksum_sup[1]
            ))

        return self.classes[msg_cls].parse(msg_id, buff[4:])

    @staticmethod
    def _read_until(stream, terminator, size=None):
        u"""Read from the stream until the terminator byte/s are read.
        Return the bytes read including the termination bytes.
        """
        term_len = len(terminator)
        line = bytearray()
        while True:
            c = stream.read(1)
            if c:
                line += c
                if line[-term_len:] == terminator:
                    break
                if size is not None and len(line) >= size:
                    break
            else:
                break

        return str(line)

    @staticmethod
    def _generate_fletcher_checksum(payload):
        u"""Return the checksum for the provided payload"""
        check_a = 0
        check_b = 0

        for char in payload:
            check_a += ord(char)
            check_a &= 0xFF

            check_b += check_a
            check_b &= 0xFF

        return str((check_a, check_b))
