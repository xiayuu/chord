#!/usr/bin/env python
# encoding: utf-8

from chord import ChordProtocol

class ChordClient(ChordProtocol):

    """node_address: (ipaddr, port)"""
    def __init__(self, node_address):
        self.node_address = node_address

    """return value or None"""
    def get(self, key)
        res = self.get_key(self.node_address, key)
        return res

    """return ok or failed"""
    def set(self, key, val):
        res = add_key(self.node_address, key, val)
        return res

    """return ok or failed"""
    def remove(self, key):
        res = self.delete_key(self,node_address, key)
        return res

