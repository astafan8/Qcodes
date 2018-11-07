import os
from pathlib import PurePath
from time import perf_counter, sleep
from queue import Queue
import argparse
import json
from typing import Tuple, Dict, Optional
from _io import TextIOWrapper
import logging
from threading import Thread

import zmq
from zmq.sugar.socket import Socket


# CONSTANTS
WRITE_ROW_ARTIFICIAL_SLEEP = 1  # longer than Measurer ping timeout is dangerous
DEFAULT_PING_TIMEOUT = 15
FILEMODES = {'GNUPLOT': {'extension': '.dat'}}
DEFAULT_FILE_MODE = list(FILEMODES.keys())[0]
DIR_FOR_DATAFILE = PurePath(os.path.realpath(__file__)).parent


logger = logging.getLogger('writer')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('writerslog.log')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s *|* %(levelname)s *|*'
                              ' %(funcName)s *|* %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


class Writer:
    """
    The object that holds the state of the current write process
    """

    def __init__(self, pull_port: int, rep_port: int):

        logger.info(f'PULL port: {pull_port}, REP port: {rep_port}')

        ctx = zmq.Context()
        self.pull_socket: Socket = ctx.socket(zmq.PULL)
        self.pull_socket.connect(f'tcp://127.0.0.1:{pull_port}')
        self.rep_socket: Socket = ctx.socket(zmq.REP)
        self.rep_socket.bind(f'tcp://127.0.0.1:{rep_port}')
        self.poller = zmq.Poller()
        self.poller.register(self.rep_socket)
        self.poller.register(self.pull_socket)

        self.last_ping = perf_counter()
        self.timeout = DEFAULT_PING_TIMEOUT  # this is re-configured on the REQ-REP line

        # The state of the run that needs to be passed around between different
        # sub-writers
        self.guid = ''
        self.mode: str = DEFAULT_FILE_MODE
        self.columns: Tuple = ()  # the ORDERED column names of the data
        self.filename: str = 'writer_default'
        self.filehandle: Optional[TextIOWrapper] = None

        # New threading feature o.O O.o
        self.mssg_queue: Queue = Queue()
        self.keep_writer_thread_alive = True
        self.write_thread = Thread(target=self._off_main_thread_writer,
                                   args=())
        self.write_thread.start()

        logger.info('init OK')

    def _off_main_thread_writer(self) -> None:
        logger.info('Off-thread writer started')

        try:

            while self.keep_writer_thread_alive:
                if not self.mssg_queue.empty():
                    mssgdict = self.mssg_queue.get()
                    guid = mssgdict['metadata']['guid']
                    chunkid = mssgdict['metadata']['chunkid']
                    datatuple = mssgdict['data']

                    if self.guid != guid:
                        logger.info("Got new guid, opening new file")
                        self.guid = guid
                        if self.filehandle is not None:
                            self.filehandle.flush()
                            self.filehandle.close()
                        self.filename = os.path.join(
                            DIR_FOR_DATAFILE,
                            self.guid + FILEMODES[self.mode]['extension']
                        )
                        self.filehandle = open(self.filename, 'a')
                        self.columns = tuple(tup[0] for tup in datatuple)

                    if chunkid == 1:
                        logger.info("Writing header")
                        # Some file modes have an extra step on first write
                        # (like writing a header)
                        if self.mode == 'GNUPLOT':
                            self.gnuplot_write_header()

                    # now write a line
                    # there should be a switch-dict here
                    if self.mode == 'GNUPLOT':
                        logger.info(f'Writing chunk number {chunkid} to GUID {guid}')
                        self.gnuplot_write_row(datatuple)

                    self.last_ping = perf_counter()
                    self.mssg_queue.task_done()
                else:
                    pass
        except:
            logger.exception('Exception in writer sub-thread', exc_info=True)

        logger.info('Off-thread writer signing off')

    def handle_data_message(self) -> None:
        """
        To be called when a poll has revealed that there is something
        in the PULL queue
        """
        mssgdict = self._receive_data()
        self.mssg_queue.put(mssgdict)
        self.last_ping = perf_counter()

    def gnuplot_write_header(self) -> None:
        """
        Write the header
        """
        line = ' '.join(self.columns) + '\n'
        self.filehandle.write(line)

    def gnuplot_write_row(self, datatuple: Tuple) -> None:
        """
        Append a row to a gnuplot file. For now only handles single points

        The result tuple must be sorted and have the correct size.
        Nulls are needed where data is missing
        """
        # to ensure writing the correct number in the correct column,
        # we must sort the input
        sorted_data = sorted(datatuple, key=lambda x: self.columns.index(x[0]))
        datapoints = tuple(tup[1] for tup in sorted_data)

        line = " ".join([str(datum) for datum in datapoints]) + "\n"
        self.filehandle.write(line)
        self.filehandle.flush()

        # sleeping longer than the Measurer ping timeout is dangerous
        sleep(WRITE_ROW_ARTIFICIAL_SLEEP)

    def handle_ping_request(self) -> None:
        """
        Handle the ping and get the configuration that sets the timeout
        """
        logger.info('Got a ping')
        mssg = self.rep_socket.recv().decode('utf-8')
        conf = json.loads(mssg)
        self.rep_socket.send(b" ")  # ping back that we are alive
        self.timeout = conf['timeout'] + 1  # to be safe
        self.last_ping = perf_counter()

    def _receive_data(self) -> Dict:
        """
        Pull data out of the socket and pass it on
        """
        logger.info('Receiving data')
        metadata = self.pull_socket.recv_json()
        data = self.pull_socket.recv_pyobj()
        return {'metadata': metadata, 'data': data}


def main(pull_port: int, rep_port: int) -> None:
    """
    This function is the main event loop. It does everything via a Writer
    instance.
    Pulls for data, writes it to disk, dies after a long dead time

    Args:
        pull_port: port number for PULL socket
        rep_port: port number for REP socket
    """

    writer = Writer(pull_port, rep_port)

    logger.info('starting the listening event loop')

    while not (perf_counter() - writer.last_ping) > writer.timeout:

        response = dict(writer.poller.poll(timeout=100))

        # on the REQ-REP line, configuration dicts are being sent
        if writer.rep_socket in response:
            writer.handle_ping_request()

        if writer.pull_socket in response:
            writer.handle_data_message()

    logger.info('Writer done waiting and working')

    writer.keep_writer_thread_alive = False
    writer.write_thread.join()
    logger.info('Write thread finished')

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Be a suicidal writer')
    parser.add_argument('pull_port', metavar='pull_port', type=int,
                        help='port to pull for data')
    parser.add_argument('rep_port', metavar='rep_port', type=int,
                        help='port to reply to with status')
    args = parser.parse_args()
    pull_port = args.pull_port
    rep_port = args.rep_port

    main(pull_port, rep_port)
