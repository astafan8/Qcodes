import zmq
from time import sleep

from numpy.random import rand

topic = "TEST"

port = 5555
ctx = zmq.Context()

# make the REQ socket to confirm that the subscriber is ready
rep_sock = ctx.socket(zmq.REP)
rep_sock.bind(f"tcp://127.0.0.1:{port+1}")

# make the PUB socket for the data queue
socket = ctx.socket(zmq.PUB)
socket.bind(f"tcp://127.0.0.1:{port}")

# make the PULL socket to read back status from the reader
pull_sock = ctx.socket(zmq.PULL)
pull_sock.connect(f"tcp://127.0.0.1:{port+2}")

# Make a poller to allow a non-blocking status check
poller = zmq.Poller()
poller.register(pull_sock, zmq.POLLIN)

if __name__ == "__main__":

    # do the syncing with the subscriber
    rep_sock.recv()

    rep_sock.send(b"")

    # here goes the messaging part
    for n in range(1, 51):
        mssg = f"{topic},this is message {n}"
        socket.send(mssg.encode('utf-8'))
        print(f"Just sent message {n}")
        # now acquire data, that takes some time
        sleep(rand())  # if we don't sleep here, we are "too fast"
        socks = dict(poller.poll(10))  # 10 ms timeout
        if pull_sock in socks and socks[pull_sock] == zmq.POLLIN:
            status = int(pull_sock.recv().decode('utf-8'))
            # print(f'Got status: {status}')
            while n - status > 3:
                status = int(pull_sock.recv().decode('utf-8'))
                print('Reader is too far behind. Waiting.')

    print("I am done sending messages")
    killmssg = f"{topic},KILL"
    socket.send(killmssg.encode('utf-8'))

    # Now we have to make sure that our bro is done
    while n - status > -1:  # -1 because of "KILL"
        status = int(pull_sock.recv().decode('utf-8'))
        print(f'Ensuring that reader is catching up: {n -status} behind')

    for sock in [socket, pull_sock, rep_sock]:
        sock.close()