#!/usr/bin/env python
# encoding: utf-8

from hashlib import sha1
from rpcudp.rpcserver import RPCServer, rpccall
from utils import Log, period_task

log = Log().getLogger()

RING_SIZE = 32

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
        self.report_state()

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

    @rpccall
    def update_finger_table(self, dest, node, finger_id):
        pass

    @rpccall
    def pop_preceding_keys(self, dest, ident):
        pass

    @period_task(period=5)
    def report_state(self):
        log.info(self.dict())

    def hash_key(self, key):
        return int(sha1(key).hexdigest(), 16)/pow(2, 128)

    def hash_node(self, address):
        return int(sha1(address[0]).hexdigest(), 16)/pow(2, 128)

    @property
    def ident(self):
        if self._ident:
            return self._ident
        else:
            self._ident = self.hash_node(self.address)
            return self._ident

    def dict(self):
        if self.successor:
            self.successor['ident'] = self.successor['ident']

        return {'address': self.address, 'ident':self.ident,
                'successor': self.successor, 'predecessor': self.predecessor}


    def rpc_rdict(self):
        return self.dict()

    def rpc_closest_preceding_finger(self, ident):
        i = RING_SIZE -1
        while i >= 0:
            node = self.finger_table[i]['node']
            if circular_range(node['ident'], self.ident, ident):
                node = self.rdict(node['address'])
                return node
            i -= 1

        node = self.dict()
        return node

    def join(self, successor_addr=None):
        if successor_addr:
            node = self.find_successor(successor_addr, self.ident)
            self.successor = {
                'address': node['address'],
                'ident': node['ident']
            }

        for i in range(RING_SIZE):
            self.finger_table.append({
                'start': (self.ident + (pow(2, i))) % pow(2, RING_SIZE)
            })

        self.create_finger_table()
        log.info(self.dict())
#        log.info(self.finger_table)

    def rpc_find_successor(self, ident):
        pred = self.rpc_find_predecessor(ident)

        if pred['successor']['ident'] == self.ident:
            return self.dict()
        else:
            return pred['successor']

    def rpc_pop_preceding_keys(self, ident):
        return_keys = {}

        for key in self.keys.keys():
            if self.hash_key(key) <= ident:
                return_keys.update({key: self.keys.pop(key)})
        return return_keys

    def create_finger_table(self):
        if self.successor:
            node = self.find_successor(self.successor['address'],
                                       self.finger_table[0]['start'])
            self.successor = node
            self.finger_table[0]['node'] = node
            node = self.rdict(self.successor['address'])
            self.predecessor = node['predecessor']

            self.update_predecessor(node['address'], self.dict())
            for i in range(RING_SIZE - 1):
                start = self.finger_table[i + 1]['start']

                if circular_range(start, self.ident,
                                  self.finger_table[i]['node']['ident']):
                    self.finger_table[i + 1]['node'] = self.finger_table[i]['node']
                else:
                    self.finger_table[i + 1]['node'] = self.find_successor(
                        self.successor['address'], self.finger_table[i + 1]['start'])

            self.update_others(self.dict())

            keys = self.pop_preceding_keys(self.successor['address'], self.ident)

            for key in keys:
                self.keys[key] = keys[key]
        else:
            for i in range(RING_SIZE):
                self.finger_table[i]['node'] = self.dict()
            successor = {
                'address': self.address,
                'ident': self.ident}
            self.successor = self.predecessor = successor

    def update_others(self, node):
        for i in range(RING_SIZE):
            ind = (self.ident - pow(2, i)) % pow(2, RING_SIZE)
            p = self.rpc_find_predecessor(ind)
            if p['ident'] != self.ident:
                self.update_finger_table(p['address'], node, i)

    def rpc_update_finger_table(self, node, finger_id):
        if self.ident == node['ident']:
            self.finger_table[finger_id]['node'] = node
            if finger_id == 0:
                self.successor = {
                    'address': node['address'],
                    'ident': node['ident']}

        elif circular_range(node['ident'], self.ident,
                            self.finger_table[finger_id]['node']['ident']):
            self.finger_table[finger_id]['node'] = node
            if finger_id == 0:
                self.successor = {
                    'address': node['address'],
                    'ident': node['ident']}

    def rpc_update_predecessor(self, node):
        self.predecessor = {
            'address': node['address'],
            'ident': node['ident']}
        return 0

    def rpc_find_predecessor(self, ident):
        node = self.dict()
        node_id = node['ident']
        successor = self.successor
        while not circular_range(ident, node_id, successor['ident']):
            if node_id == self.ident:
                node = self.rpc_closest_preceding_finger(ident)
                if node['ident'] == self.ident:
                    break
            else:
                node = self.closest_preceding_finger(node['address'], ident)

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

