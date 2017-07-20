#!/usr/bin/env python
# encoding: utf-8

from protocol import ChordProtocol
from utils import args

if not args.bind:
    print("args error")

ip, port = args.bind.split(':')
bind = (ip, int(port))

node = ChordProtocol(bind)
if args.peer:
    ip, port = args.peer.split(':')
    dest = (ip, int(port))
    node.join(dest)
else:
    node.join()

node.run(bind)
