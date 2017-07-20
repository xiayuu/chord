#!/usr/bin/env python
# encoding: utf-8

from hashlib import sha1
from rpcudp.rpcserver import RPCServer, rpccall
from utils import Log

log = Log().getLogger()

RING_SIZE = 160

def circular_range(i, p1, p2):
    if p1 < p2:
        return i > p1 and i < p2
    else:
        return (i > p1 and i > p2) or (i < p1 and i < p2)

class ChordProtocol(RPCServer):

    def __init__(self, address):
        super(ChordProtocol, self).__init__()
        self.finger_table = []
        self.keys = {}
        self.address = address
        self._ident = None
        self.successor = None
        self.predecessor = None

    @rpccall
    def get_key(self, dest, key):
        pass

    @rpccall
    def add_key(self, dest, key, value):
        pass

    @rpccall
    def delete_key(self, dest, key):
        pass

    @rpccall
    def closest_preceding_finger(self, dest, ident):
        pass

    @rpccall
    def rdict(self, dest):
        pass

    @rpccall
    def find_successor(self, dest, ident):
        pass

    @rpccall
    def find_predecessor(self, dest, ident):
        pass

    @rpccall
    def update_predecessor(self, dest, node):
        pass

    def hash_key(self, key):
        return int(sha1(key).hexdigest(), 16)

    def hash_node(self, address):
        return int(sha1(address[0]).hexdigest(), 16)

    @property
    def ident(self):
        if self._ident:
            return self._ident
        else:
            self._ident = self.hash_node(self.address)
            return self._ident

    def dict(self):
        if self.successor:
            self.successor['ident'] = str(self.successor['ident'])

        return {'address': self.address, 'ident':str(self.ident),
                'successor': self.successor, 'predecessor': self.predecessor}


    def rpc_rdict(self):
        return self.dict()

    def rpc_closest_preceding_finger(self, ident):
        i = RING_SIZE -1
        while i >= 0:
            node = self.finger_table[i]['node']
            if circular_range(int(node['ident']), int(self.ident), int(ident)):
                node = self.rdict(node['address'])
                return node
            i -= 1

        node = self.dict()
        return node

    def join(self, successor_addr=None):
        if successor_addr:
            node = self.find_successor(successor_addr, str(self.ident))
            self.successor = {
                'address': node['address'],
                'ident': node['ident']
            }

        for i in range(RING_SIZE):
            self.finger_table.append({
                'start': int((self.ident + (pow(2, i-1))) % pow(2, RING_SIZE))
            })

        self.create_finger_table()
        log.info(self.dict())
        log.info(self.finger_table)

    def rpc_find_successor(self, ident):
        ident = int(ident)

        pred = self.rpc_find_predecessor(ident)
        pred['ident'] = int(pred['ident'])
        pred['successor']['ident'] = str(pred['successor']['ident'])

        if pred['successor']['ident'] == self.ident:
            return self.dict()
        else:
            return pred['successor']

    def create_finger_table(self):
        for i in range(RING_SIZE):
            self.finger_table[i]['node'] = self.dict()
        successor = {
            'address': self.address,
            'ident': self.ident}
        self.successor = self.predecessor = successor

    def rpc_update_predecessor(self, node):
        self.predecessor = {
            'address': node['address'],
            'ident': node['ident']}
        return 0

    def rpc_find_predecessor(self, ident):
        node = self.dict()
        node_id = node['ident']
        successor = self.successor
        while not circular_range(int(ident), int(node_id), int(successor['ident'])):
            if int(node_id) == self.ident:
                node = self.closest_preceding_finger(ident)
                if int(node['ident']) == int(self.ident):
                    continue
            else:
                node = self.closest_preceding_finger(node['address'], str(ident))

            node_id = node['ident']
            successor = node['successor']

        return node

    def rpc_getkey(self, key):
        key_ident = self.hash_key(key)
        node = self.find_successor(key_ident)

        if int(node['ident']) == int(self.ident):
            addr = self.address[0]
            if key in self.keys:
                res = self.keys[key]
            else:
                res = None
            return {'node': addr, 'res': res}
        else:
            return self.get_key(node['address'], key)

