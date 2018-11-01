import multiprocessing as mp
from time import sleep, perf_counter

import sys
import zmq

# This is the main (mother) process, also the pusher


def suicidal_writer(pull_port: int, rep_port: int) -> None:
    """
    Listen for stuff and die after some time
    """

    print('WRITER REPORTING ONLINE!')
    print(f'WRITER CONNECTING PULL ON {pull_port} AND BINDING REP ON {rep_port}')

    ctx = zmq.Context()

    # make the PULL socket we'll use for receiving the data
    pull_sock = ctx.socket(zmq.PULL)
    pull_sock.connect(f"tcp://127.0.0.1:{pull_port}")

    # make the REP socket that we'll use for handshaking
    rep_sock = ctx.socket(zmq.REP)
    rep_sock.bind(f"tcp://127.0.0.1:{rep_port}")

    poller = zmq.Poller()
    poller.register(rep_sock)
    poller.register(pull_sock)

    last_ping = perf_counter()

    timeout = 15  # timeout in seconds

    while not (perf_counter() - last_ping) > timeout:

        response = dict(poller.poll(timeout=100))

        if rep_sock in response:
            mssg = rep_sock.recv().decode('utf-8')
            print(f'Got a message on the handshake line: {mssg}')
            rep_sock.send(b"Yes")
            last_ping = perf_counter()
        if pull_sock in response:
            mssg = pull_sock.recv()
            print(f"Got a message on the main line: {mssg.decode('utf-8')}")
            last_ping = perf_counter()

    print('Writer exeeded its timeout and died')


port = 5557
ctx = zmq.Context()

if __name__ == "__main__":

    # make the PUSH socket we'll use for pushing out data
    push_sock = ctx.socket(zmq.PUSH)
    push_sock.set_hwm(1)

    # make the REQ socket we'll use for handshaking with the writer
    req_sock = ctx.socket(zmq.REQ)

    got_port = False
    for n in range(1, 12, 2):
        print(f'Trying to bind pusher and req on ports {port}, {port+1}')
        try:
            push_sock.bind(f"tcp://127.0.0.1:{port}")
            req_sock.connect(f"tcp://127.0.0.1:{port+1}")
            got_port = True
            break
        except:
            port += 1

    if not got_port:
        print('No port available')
        sys.exit()

    poller = zmq.Poller()
    poller.register(req_sock, zmq.POLLIN)

    print('Asking writer if it is alive...')

    # now check whether we should spawn the writer
    req_sock.send(b'Are you there?')

    response = dict(poller.poll(timeout=100))  # timeout in ms

    if req_sock in response:
        print(f'Got poll response: {response}')
        print(req_sock.recv())
    else:
        print('No writer found')
        print("spawning writer...")

        # then delete the socket and reopen it
        req_sock.setsockopt(zmq.LINGER, 0)
        req_sock.close()
        poller.unregister(req_sock)

        writer = mp.Process(target=suicidal_writer, args=(port, port+1))

        writer.start()

        req_sock = ctx.socket(zmq.REQ)
        req_sock.connect(f"tcp://127.0.0.1:{port+1}")
        poller.register(req_sock, zmq.POLLIN)
        sleep(0.250)
        print('Checking that the writer is online')

        # now check whether we should spawn the writer
        req_sock.send(b'Are you there?')

        response = dict(poller.poll(timeout=1000))  # timeout in ms

        if req_sock in response:
            print(f'Got poll response: {response}')
            print(req_sock.recv())
        else:
            print('Could not spawn writer, exiting')
            sys.exit()


    for n in range(5):
        message = f'This is message {n+1}'
        push_sock.send(message.encode('utf-8'))
        sleep(0.5)
        print(f'Just sent message number {n+1}')

    print("Spawner is dead")