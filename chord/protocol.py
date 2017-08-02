#!/usr/bin/env python
# encoding: utf-8

from hashlib import sha1
from rpcudp.rpcserver import RPCServer, rpccall
from utils import Log, period_task, delay_run

log = Log().getLogger()

RING_SIZE = 4

# p2<i<p1 or p1<i<p2
def circular_range_nab(i, p1, p2):
    if p1 < p2:
        return i < p2 and i > p1
    if p1 > p2:
        return i > p1 or i < p2
    return False

# p2<=i<p1 or p1<=i<p2 or i = p1 = p2
def circular_range_a(i, p1, p2):
    if i == p1 and p1 != p2:
        return True
    else:
        return circular_range_nab(i, p1, p2)

# p2<i<=p1 or p1<i<=p2 or i = p1 = p2
def circular_range_b(i, p1, p2):
    if i == p2 and p1 != p2:
        return True
    else:
        return circular_range_nab(i, p1, p2)

class ChordProtocol(RPCServer):

    def __init__(self, address):
        super(ChordProtocol, self).__init__(DEBUG=False)
        self.finger_table = []
        self.keys = {}
        self.address = address
        self._ident = None
        self.successor = None
        self.predecessor = None
        self.init_ring()
        self.report_state()
        self.stabilize()
        self.fix_fingers()

    def run_chord(self, peer=None):
        if peer:
            self.join(peer)
        self.run(self.address)

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

    @rpccall
    def notify(self, dest, node):
        pass

    @period_task(period=15)
    def report_state(self):
        log.info("ident:%d,successor:%d,predecessor:%d" %
                 (self.ident, self.successor['ident'], self.predecessor['ident']))
        i = 1
        for f in self.finger_table:
            log.info("%d: start:%d,node.ident:%d" %
                     (i, f['start'], f['node']['ident']))
            i = i + 1
        log.info(self.keys)

    def hash_key(self, key):
        return int(sha1(key).hexdigest(), 16)/pow(2, 160 - RING_SIZE)

    def hash_node(self, address):
        return int(sha1(address[0]).hexdigest(), 16)/pow(2, 160 - RING_SIZE)

    @property
    def ident(self):
        if self._ident:
            return self._ident
        else:
            self._ident = self.hash_node(self.address)
            return self._ident

    def dict(self):
        return {'address': self.address, 'ident':self.ident,
                'successor': self.successor, 'predecessor': self.predecessor}

    def rpc_rdict(self):
        return self.dict()

    def rpc_closest_preceding_finger(self, ident):
        i = RING_SIZE -1
        while i >= 0:
            node = self.finger_table[i]['node']
            if circular_range_nab(node['ident'], self.ident, ident):
                return node
            i -= 1

        node = self.dict()
        return node

    def init_ring(self):
        self.successor = self.predecessor = {'ident': self.ident,'address': self.address}
        for i in range(RING_SIZE):
            self.finger_table.append({
                'start': (self.ident + (pow(2, i))) % pow(2, RING_SIZE),
                'node': self.dict()})

    @delay_run(delay=5)
    def join(self, successor_addr):
        self.init_finger_table(successor_addr)
        self.update_others()
        keys = self.pop_preceding_keys(self.successor['address'], self.ident)
        for key in keys:
            self.keys[key] = keys[key]

    @period_task(period=10)
    def stabilize(self):
        x = self.rdict(self.successor['address'])['predecessor']
        if circular_range_nab(x['ident'], self.ident, self.successor['ident']):
            self.successor['ident'] = x['ident']
            self.successor['address'] = x['address']
        self.notify(self.successor['address'], self.dict())

    def rpc_notify(self, node):
        if circular_range_nab(node['ident'], self.predecessor['ident'], self.ident):
            self.predecessor['ident'] = node['ident']
            self.predecessor['address'] = node['address']

    @period_task(period=15)
    def fix_fingers(self):
        for i in range(RING_SIZE):
            self.finger_table[i]['node'] = self.rpc_find_successor(
                self.finger_table[i]['start'])

    def rpc_find_successor(self, ident):
        if ident == self.ident:
            return self.dict()
        pred = self.rpc_find_predecessor(ident)
        return self.rdict(pred['successor']['address'])

    def rpc_pop_preceding_keys(self, ident):
        return_keys = {}

        for key in self.keys.keys():
            if self.hash_key(key) <= ident:
                return_keys.update({key: self.keys.pop(key)})
        return return_keys

    def init_finger_table(self, successor_addr):
        self.finger_table[0]['node'] = self.find_successor(successor_addr,
                                             self.finger_table[0]['start'])
        self.successor['ident'] = self.finger_table[0]['node']['ident']
        self.successor['address'] = self.finger_table[0]['node']['address']
        self.predecessor = self.finger_table[0]['node']['predecessor']

        self.update_predecessor(self.successor['address'], self.dict())

        for i in range(RING_SIZE - 1):
            start = self.finger_table[i + 1]['start']
            if circular_range_a(start, self.ident,
                              self.finger_table[i]['node']['ident']):
                self.finger_table[i + 1]['node'] = self.finger_table[i]['node']
            else:
                self.finger_table[i + 1]['node'] = self.find_successor(successor_addr,
                                                                       start)

    def update_others(self):
        for i in range(RING_SIZE):
            ind = (self.ident - pow(2, i)) % pow(2, RING_SIZE)
            p = self.rpc_find_predecessor(ind)
            if p['ident'] != self.ident:
                self.update_finger_table(p['address'], self.dict(), i)

    def rpc_update_finger_table(self, node, finger_id):
        if self.ident == self.successor['address']:
            self.successor['ident'] = node['ident']
            self.successor['address'] = node['address']
            self.finger_table[finger_id]['node'] = node
            return

        if circular_range_a(node['ident'], self.ident,
                            self.finger_table[finger_id]['node']['ident']):
            if finger_id == 0:
                self.successor['ident'] = node['ident']
                self.successor['address'] = node['address']
            self.finger_table[finger_id]['node'] = node
            self.update_finger_table(self.predecessor['address'], node, finger_id)

    def rpc_update_predecessor(self, node):
        self.predecessor = {
            'address': node['address'],
            'ident': node['ident']}
        if self.ident == self.successor['ident']:
            self.successor = self.predecessor
        return 0

    def rpc_find_predecessor(self, ident):
        node = self.dict()
        node_id = node['ident']
        successor = self.successor
        while not circular_range_b(ident, node_id, successor['ident']):
            node = self.closest_preceding_finger(node['address'], ident)
            if node_id == node['ident']:
                return node
            node_id = node['ident']
            successor = node['successor']
        return node

    def rpc_get_key(self, key):
        key_ident = self.hash_key(key)
        node = self.rpc_find_successor(key_ident)

        if node['ident'] == self.ident:
            if key in self.keys:
                res = self.keys[key]
            else:
                res = None
            return res
        else:
            return self.get_key(node['address'], key)

    def rpc_add_key(self, key, val):
        key_ident = self.hash_key(key)
        node = self.rpc_find_successor(key_ident)
        if node['ident'] == self.ident:
            self.keys[key] = val
            return "OK"
        else:
            return self.add_key(node['address'], key, val)

    def rpc_delete_key(self, key):
        key_ident = self.hash_key(key)
        node = self.rpc_find_successor(key_ident)

        if node['ident'] == self.ident:
            self.keys.pop(key)
            return "OK"
        else:
            return self.delete_key(node['address'], key)
