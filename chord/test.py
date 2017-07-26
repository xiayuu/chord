#!/usr/bin/env python
# encoding: utf-8

from hashlib import sha1
from rpcudp.rpcserver import RPCServer, rpccall


RING_SIZE = 4


class ChordProtocol(RPCServer):

    @rpccall
    def rdict(self, dest):
        pass

    @rpccall
    def find_successor(self, dest, ident):
        pass

    def rpc_rdict(self):
        return self.dict()

node = ChordProtocol()
print(node.find_successor(('192.168.1.4', 9001), 10))
