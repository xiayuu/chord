#!/usr/bin/env python
# encoding: utf-8

import logging, logging.handlers
import argparse
from hashlib import sha1

parser = argparse.ArgumentParser()

parser.add_argument('-b', '--bind', help="ipaddr:port this listen to")
parser.add_argument('-p', '--peer', help="ipaddr:port node to connect")
parser.add_argument('-s', '--logserver', help='ipaddr:port log server to connect')
parser.add_argument('-l', '--loglevel', help='0,1,2 means error, info, debug', type=int)
args = parser.parse_args()

LOGLEVEL = (logging.ERROR,
            logging.INFO,
            logging.DEBUG)

class Singleton(object):
    _state = {}
    def __new__(cls, *args, **kw):
        ob = super(Singleton, cls).__new__(cls, *args, **kw)
        ob.__dict__ = cls._state
        return ob

class Log(Singleton):
    def setLogger(self, dest, loglevel, logname):
        self.logger = logging.getLogger(logname)
        self.logger.setLevel(LOGLEVEL[loglevel])
        sockethander = logging.handlers.SocketHandler(dest[0], int(dest[1]))
        self.logger.addHandler(sockethander)

    def getLogger(self):
        return self.logger

Log().setLogger(args.logserver.split(':'), args.loglevel,
                str(sha1(args.bind.split(':')[0]).hexdigest()))
