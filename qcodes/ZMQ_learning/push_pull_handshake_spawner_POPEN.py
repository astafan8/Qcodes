import os
import subprocess
from pathlib import PurePath
from time import sleep, perf_counter
from typing import Tuple

import sys
import zmq
from zmq.sugar.socket import Socket  # for type annotations


SPAWNER_DIR = PurePath(os.path.realpath(__file__)).parent
SPAWNER_FILENAME = 'push_pull_handshake_suicidal_writer.py'

# This is the main (mother) process, also the pusher

def is_the_writer_online(poller: zmq.Poller,
                         req_sock: Socket,
                         timeout: float) -> bool:
    """
    Ask the writer for a reply

    Args:
        poller: a Poller with the req socket
        req_sock: the req socket. Should be in poller
        timeout: poll timeout in SECONDS
    """

    print('Asking writer "are you alive?"')

    # now check whether we should spawn the writer
    req_sock.send(b'Are you there?')

    response = dict(poller.poll(timeout=1000*timeout))  # timeout in ms

    if req_sock in response:
        print(req_sock.recv().decode('utf-8'))
        return True
    else:
        print('No writer found')
        return False


def reset_req_sock_and_poller(poller: zmq.Poller,
                              req_sock: Socket,
                              req_port: int) -> Tuple[zmq.Poller, Socket]:
    """
    Reset socket and poller for reuse
    """
    req_sock.setsockopt(zmq.LINGER, 0)
    req_sock.close()
    poller.unregister(req_sock)

    req_sock = ctx.socket(zmq.REQ)
    req_sock.connect(f"tcp://127.0.0.1:{req_port}")
    poller.register(req_sock, zmq.POLLIN)

    return (poller, req_sock)


def spawn_writer(pull_port: int, rep_port: int) -> None:
    """
    Spawn the writer. I dunno, should this return something?
    """
    print('Spawning out a writer')
    cmd = ["python", str(SPAWNER_DIR / SPAWNER_FILENAME),
           f"{pull_port}", f"{rep_port}"]
    p = subprocess.Popen(cmd, creationflags=DETACHED_PROCESS)
    print(f'Spawned writer with pid {p.pid}')


port = 5557
ctx = zmq.Context()
DETACHED_PROCESS = 0x00000008

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
            req_port = port+1
            break
        except:
            port += 1

    if not got_port:
        print('No port available')
        sys.exit()

    poller = zmq.Poller()
    poller.register(req_sock, zmq.POLLIN)

    # now check if the writer is alive
    # if it is, just proceed. If not, spawn it
    if not is_the_writer_online(poller, req_sock, 0.1):

        spawn_writer(port, req_port)

        # then reset the req socket, since it is now "broken" from not having
        # got a response
        poller, req_sock = reset_req_sock_and_poller(poller, req_sock,
                                                     req_port)

        sleep(1)
        if not is_the_writer_online(poller, req_sock, 1):
            print('Failing to spawn writer. Sorry, bro.')
            sys.exit()

    # if we ever make it this far, there is a writer

    for n in range(5):
        message = f'This is message {n+1}'
        push_sock.send(message.encode('utf-8'))
        sleep(0.5)
        print(f'Just sent message number {n+1}')

    print("Spawner is dead")