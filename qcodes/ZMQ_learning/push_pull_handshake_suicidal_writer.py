import zmq
from time import perf_counter
import os
from datetime import datetime
import argparse


def suicidal_writer(pull_port: int, rep_port: int) -> None:
    """
    Listen for stuff and die after some time
    """

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

    timestr = datetime.now().strftime('%Y-%m-%d_%H_%M_%S')
    filename = os.path.join(os.getcwd(), f'writer-{timestr}.txt')

    # In the real application, ANOTHER file will be opened based on message
    # content. This file is just for status "prints"
    with open(filename, 'w') as fid:

        fid.write(f'Got pull_port {pull_port} and req_port {rep_port}\n')

        while not (perf_counter() - last_ping) > timeout:

            response = dict(poller.poll(timeout=100))

            if rep_sock in response:
                mssg = rep_sock.recv().decode('utf-8')
                fid.write(mssg + '\n')
                rep_sock.send(b"Yes")
                last_ping = perf_counter()
            if pull_sock in response:
                mssg = pull_sock.recv()
                fid.write(mssg.decode('utf-8') + '\n')
                last_ping = perf_counter()

        fid.write('Reached my timeout limit. Signing off.')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Be a suicidal writer')
    parser.add_argument('pull_port', metavar='pull_port', type=int,
                        help='port to pull for data')
    parser.add_argument('rep_port', metavar='rep_port', type=int,
                        help='port to reply to with status')
    args = parser.parse_args()
    pull_port = args.pull_port
    rep_port = args.rep_port

    suicidal_writer(pull_port, rep_port)

