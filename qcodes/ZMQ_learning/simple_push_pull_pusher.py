from time import sleep

import zmq
from numpy.random import rand

port = 5555
ctx = zmq.Context()

# make the PUSH socket we'll use for pushing out data
push_sock = ctx.socket(zmq.PUSH)
push_sock.set_hwm(1)
push_sock.bind(f"tcp://127.0.0.1:{port}")

if __name__ == "__main__":

    # start pushing

    # the message size in terms of string-cast floats
    N = 10

    for n in range(15):
        header = f'Message {n:05.0f}'
        mssg = header + str(rand(N))
        push_sock.send(mssg.encode('utf-8'))
        print(f"Pushed message number {n+1}")
        sleep(rand())

    mssg = "KILL"
    push_sock.send(mssg.encode('utf-8'))

    print('Done pushing, goodbye')
