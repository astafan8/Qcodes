import os
from time import perf_counter
from typing import Optional, Tuple, Union
import zmq
from zmq.sugar.socket import Socket
import subprocess
from time import sleep
import uuid

import qcodes.ZMQ_learning  # this is the path to the writer script


class Measurer:
    """
    A class to mimick the Measurement class of QCoDes. Sends data to disk
    via a spawned process. Uses PUSH-PULL for data and REQ-REP for ensuring
    the availability of a connection

    Args:
        start_port: The port to start searching for a pair of free ports
        suicide_timeout: The timeout (s) before writers should kill themselves
    """

    DETACHED_PROCESS = 0x00000008
    _ports_to_try: int = 10  # number of ports to try to bind to in init

    def __init__(self, start_port: int,
                 suicide_timeout: Optional[float]) -> None:

        # ZMQ related variables
        self._ctx = zmq.Context()
        self._push_port: Union[None, int] = None
        self._req_port: Union[None, int] = None
        self._push_socket: Union[None, Socket] = None
        self._req_socket: Union[None, Socket] = None
        self.poller = zmq.Poller()

        # Run related variables
        self._current_chunk_number: int = 0
        self._guid = f"{'0'*8}-{'0'*4}-{'0'*4}-{'0'*4}-{'0'*12}"

        self._timeout = suicide_timeout
        self._last_write = 0

        # start-up actions
        self._create_sockets(start_port)

        modulepath = qcodes.ZMQ_learning.__file__.replace('__init__.py', '')
        self._path_to_writer = os.path.join(modulepath, 'writer.py')

    def _create_sockets(self, start_port: int) -> None:
        """
        Creates the PUSH and PULL sockets and assigns ports to them.
        """

        # The PUSH socket for sending the data across
        push_sock = self._ctx.socket(zmq.PUSH)
        push_sock.set_hwm(1)

        # The REQ socket we'll use for handshaking with the writer
        req_sock = self._ctx.socket(zmq.REQ)

        got_push_port = False
        for n in range(self._ports_to_try):
            try:
                push_sock.bind(f"tcp://127.0.0.1:{start_port+n}")
                self._push_port = start_port + n
                got_push_port = True
                break
            except zmq.ZMQError:
                pass
        got_req_port = False
        for n in range(1, self._ports_to_try+1):
            try:
                req_sock.connect(f"tcp://127.0.0.1:{self._push_port+n}")
                self._req_port = self._push_port+n
                break
            except zmq.ZMQError:
                pass
        if not(got_push_port or got_req_port):
            raise RuntimeError('Could not find any unused ports to bind to. '
                               f'Tried from {start_port} to '
                               f'{start_port+self._ports_to_try}.')
        else:
            self._push_socket = push_sock
            self._req_socket = req_sock
            self.poller.register(self._push_socket, zmq.POLLIN)

    def start_run(self) -> None:
        """
        Mocks the start of a new run
        """
        self._guid = str(uuid.uuid4())
        self._current_chunk_number = 0

    def check_for_writer(self) -> bool:
        self._req_socket.send(b'Are you there?')
        response = dict(self.poller.poll(timeout=100))  # magic number

        if self._req_socket in response:
            return True
        else:
            return False

    def _spawn_writer(self) -> None:
        cmd = ["python", self._path_to_writer,
               f"{self._push_port}", f"{self._req_port}"]
        subprocess.Popen(cmd, creationflags=self.DETACHED_PROCESS)
        sleep(0.01)  # TODO: is any sleep required?

    def _reset_sockets(self) -> None:
            self._req_socket.setsockopt(zmq.LINGER, 0)
            self._req_socket.close()
            self.poller.unregister(self._req_socket)

            self._req_socket = self._ctx.socket(zmq.REQ)
            self._req_socket.connect(f"tcp://127.0.0.1:{self._req_port}")
            self.poller.register(self._req_socket, zmq.POLLIN)

    def add_result(self, result: Tuple) -> None:

        if perf_counter() - self._last_write > self._timeout:
            if not self.check_for_writer():
                self._spawn_writer()
                self._reset_sockets()
                if not self.check_for_writer():
                    raise RuntimeError('Could not spawn writer. Call 911.')

        self.send(result)

    def send(self, result: Tuple) -> None:
        header = {'guid': self._guid, 'chunkid': self._current_chunk_number}
        self._push_socket.send_json(header, zmq.SNDMORE)
        self._push_socket.send_pyobj(result)

    @property
    def guid(self):
        return self._guid

    @property
    def push_socket(self):
        return self._push_socket

    @property
    def req_sock(self):
        return self._req_socket

    @property
    def push_port(self):
        return self._push_port

    @property
    def req_port(self):
        return self._req_port

