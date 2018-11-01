from time import sleep

import zmq
from numpy.random import rand, randint

port = 5555
ctx = zmq.Context()

# make the PUSH socket we'll use for pushing out data
pull_sock = ctx.socket(zmq.PULL)
pull_sock.connect(f"tcp://127.0.0.1:{port}")

if __name__ == "__main__":

    # pull messages

    time_to_die = False

    while not(time_to_die):
        raw_mssg = pull_sock.recv()
        raw_mssg = raw_mssg[:13]
        mssg = raw_mssg.decode('utf-8')
        print(f"Got message: '{mssg}'")
        sleep(2*rand())
        if mssg == "KILL":
            time_to_die = True
            print('Got kill signal, goodbye')
        if randint(0, 10) > 6:
            print('Oh shit, I am crashing, goodbye')
            break

