#!/usr/bin/env python
# encoding: utf-8

from rpcudp.rpcserver import RPCServer, rpccall
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("host", help="node hostname")
parser.add_argument("port", help="port")
parser.add_argument("action", help="get , set, remove")
parser.add_argument("--key", help="key")
parser.add_argument("--value", help="value")

args = parser.parse_args()

class ChordClient(RPCServer):

    """node_address: (ipaddr, port)"""
    def __init__(self, node_address):
        super(ChordClient, self).__init__(DEBUG=True)
        self.node_address = node_address

    @rpccall
    def get_key(self, dest, key):
        pass

    @rpccall
    def add_key():
        pass

    @rpccall
    def delete_key():
        pass

    """return value or None"""
    def get(self, key):
        res = self.get_key(self.node_address, key)
        return res

    """return ok or failed"""
    def set(self, key, val):
        res = self.add_key(self.node_address, key, val)
        return res

    """return ok or failed"""
    def remove(self, key):
        res = self.delete_key(self.node_address, key)
        return res

client = ChordClient((args.host, int(args.port)))
if args.action == "get":
    print(client.get(args.key))

if args.action == "set":
    client.set(args.key, args.value)

if args.action == "remove":
    client.remove(args.key)
