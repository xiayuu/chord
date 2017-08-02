# chord
a p2p framework implement chord protocol
https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf

run first node
python chordnode.py -b 192.168.1.4:9001 -s 192.168.1.2:8089 -l 2

second node
python chordnode.py -b 192.168.1.3:9002 -p 192.168.1.4:9001 -s 192.168.1.2:8089 -l 2

after 30s run other nodes

run test client to set or get values
python client.py 192.168.1.2 9003 set --key name --value xiayu
