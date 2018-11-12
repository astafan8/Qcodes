import math
import os
from time import perf_counter, strftime, localtime, time
from typing import Optional, Tuple, Union, Any
import zmq
from zmq.sugar.socket import Socket
import subprocess
import uuid
import json

import qcodes.ZMQ_learning  # this is the path to the writer script
from qcodes.ZMQ_learning.common_config import DEFAULT_SUICIDE_TIMEOUT, \
    TRANSPORT_PROTOCOL, ADDRESS


WRITER_SPAWN_SLEEP_TIME = 5

PATH_TO_WRITER = os.path.join(
    qcodes.ZMQ_learning.__file__.replace('__init__.py', ''), 'writer.py')

DETACHED_PROCESS = 0x00000008


class Measurer:
    """
    A class to mimick the Measurement class of QCoDes. Sends data to disk
    via a spawned process. Uses PUSH-PULL for data and REQ-REP for ensuring
    the availability of a connection

    Args:
        start_port: The port to start searching for a pair of free ports
        suicide_timeout: The timeout (s) before writers should kill themselves
    """

    _ports_to_try: int = 10  # number of ports to try to bind to in init

    def __init__(self, start_port: int,
                 suicide_timeout: Optional[float]=None,
                 file_format: Optional[str]=None) -> None:

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
        self._file_format = file_format

        self._timeout = suicide_timeout or DEFAULT_SUICIDE_TIMEOUT
        self._last_write = perf_counter() - self._timeout - 1

        # start-up actions
        self._create_sockets(start_port)

        self._path_to_writer = PATH_TO_WRITER

        self._writer_spawn_sleep_time = WRITER_SPAWN_SLEEP_TIME

        # for testing purposes ONLY
        self._current_writer_process: Optional[subprocess.Popen] = None

        print('Init succesful')
        print(f'PUSH on {self._push_port}, REQ on {self._req_port}')

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
                push_sock.bind(
                    f"{TRANSPORT_PROTOCOL}://{ADDRESS}:{start_port+n}")
                self._push_port = start_port + n
                got_push_port = True
                break
            except zmq.ZMQError:
                pass

        got_req_port = False
        for n in range(1, self._ports_to_try+1):
            try:
                req_sock.connect(
                    f"{TRANSPORT_PROTOCOL}://{ADDRESS}:{self._push_port+n}")
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
            self.poller.register(self._req_socket, zmq.POLLIN)

    def start_run(self) -> None:
        """
        Mocks the start of a new run
        """
        self._guid = self._generate_guid()
        self._current_chunk_number = 0

    def _generate_guid(self):
        """Generate a guid (useful when testing because allows for injection)"""
        return str(uuid.uuid4())

    def check_for_writer(self, poll_timeout=100) -> bool:
        """
        This checks for writer, but also reconfigures the writer
        """
        mssg = json.dumps({'timeout': self._timeout})
        self._req_socket.send(bytes(mssg, 'utf-8'))
        # response = dict(self.poller.poll(timeout=100))  # magic number
        response = dict(self.poller.poll(
            timeout=poll_timeout))  # not a magic number anymore

        print(response)
        print(f"socket is in response: {self._req_socket in response} "
              f"({self._req_socket})")

        if self._req_socket in response:
            self._req_socket.recv()  # important to stay in-step
            return True
        else:
            return False

    def _spawn_writer(self) -> None:
        cmd = ["python", self._path_to_writer,
               f"{self._push_port}",
               f"{self._req_port}",
               f"{self._file_format}"]

        t = time()
        # extract 'sub' - seconds
        subsecs, _ = math.modf(t)
        # get milliseconds only (without microseconds and the rest)
        millisecs = int(subsecs * 1000)
        time_str = (strftime('%Y_%m_%d_%H_%M_%S', localtime(t))
                    + '_' + str(millisecs))
        print(f'Spawning writer at: {time_str}')

        self._current_writer_process = subprocess.Popen(
            cmd, creationflags=DETACHED_PROCESS)
        print(f'Spawned writer process with pid '
              f'{self._current_writer_process.pid}')
        # need for this sleep is fixed by actively polling the
        # socket for this time instead
        # sleep(WRITER_SPAWN_SLEEP_TIME)

    def _remove_req_socket(self) -> None:
        self._req_socket.setsockopt(zmq.LINGER, 0)
        self._req_socket.close()
        self.poller.unregister(self._req_socket)

    def _add_req_socket(self) -> None:
        self._req_socket = self._ctx.socket(zmq.REQ)
        self._req_socket.connect(
            f"{TRANSPORT_PROTOCOL}://{ADDRESS}:{self._req_port}")
        self.poller.register(self._req_socket, zmq.POLLIN)

    def add_result(self, result: Tuple[Tuple[str, Any]]) -> None:
        """
        Add a result to the data file

        Args:
            result: tuple of tuples ("name", value) where value can be a number
              or a string
        """
        if perf_counter() - self._last_write > self._timeout:

            if not self.check_for_writer():
                self._spawn_writer()

                self._remove_req_socket()
                self._add_req_socket()

                # adjust the polling time in order to actually wait for the
                # writer to spawn and get online
                if not self.check_for_writer(
                        poll_timeout=1000*self._writer_spawn_sleep_time):

                    self._remove_req_socket()
                    raise RuntimeError('Could not spawn writer. Call 911.')

        self._current_chunk_number += 1
        self.send_data(result)
        self._last_write = perf_counter()

    def send_data(self, result: Tuple) -> None:
        header = {'guid': self._guid, 'chunkid': self._current_chunk_number}
        self._push_socket.send_json(header, zmq.SNDMORE)
        self._push_socket.send_pyobj(result)

    @property
    def guid(self):
        return self._guid

    @property
    def timeout(self):
        return self._timeout

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
