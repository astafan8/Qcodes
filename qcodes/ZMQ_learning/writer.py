import os
from pathlib import PurePath
from time import perf_counter, localtime, strftime
from queue import Queue
import argparse
import json
from typing import Optional, Callable, Tuple
import logging
from threading import Thread, Lock

import zmq
from zmq.sugar.socket import Socket

from qcodes.ZMQ_learning.file_writers import GnuplotWriter
from qcodes.ZMQ_learning.common_config import DEFAULT_SUICIDE_TIMEOUT, ADDRESS,\
    TRANSPORT_PROTOCOL


DEFAULT_PING_TIMEOUT = DEFAULT_SUICIDE_TIMEOUT

DATA_FILE_WRITERS_MAP = {'GNUPLOT': GnuplotWriter}
DEFAULT_FILE_MODE = list(DATA_FILE_WRITERS_MAP.keys())[0]

DIR_FOR_DATAFILE = PurePath(os.path.realpath(__file__)).parent


logger = logging.getLogger('writer')
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    fh = logging.FileHandler(
        'writerslog__'
        + strftime("%Y_%m_%d_%H_%M_%S", localtime())
        + '.log')
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s *|* %(levelname)s *|*'
                                  ' %(funcName)s *|* %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)


class Writer:
    """
    The object that holds the state of the current write process

    Args:
        pull_port: port number for PULL socket
        rep_port: port number for REP socket
    """
    def __init__(self, pull_port: int, rep_port: int):

        logger.info(f'PULL port: {pull_port}, REP port: {rep_port}')

        ctx = zmq.Context()

        self.pull_socket: Socket = ctx.socket(zmq.PULL)
        self.pull_socket.connect(
            f'{TRANSPORT_PROTOCOL}://{ADDRESS}:{pull_port}')

        self.rep_socket: Socket = ctx.socket(zmq.REP)
        self.rep_socket.bind(
            f'{TRANSPORT_PROTOCOL}://{ADDRESS}:{rep_port}')

        self.poller = zmq.Poller()
        self.poller.register(self.rep_socket)
        self.poller.register(self.pull_socket)

        self.last_ping = 0

        # this is re-configured on the REQ-REP line
        self.timeout = DEFAULT_PING_TIMEOUT

        # New threading feature o.O O.o
        self.mssg_queue: Optional[Queue] = None
        self.writer_thread: Optional[WriterThread] = None

        logger.info('init OK')

    def run_message_processing_loop(self):
        """
        This function is the main event loop.

        It pulls for data, writes it to disk, dies after a long dead time.
        """
        try:
            logger.info('initializing a queue and starting a writer thread')

            self.mssg_queue: Queue = Queue()
            self.writer_thread = WriterThread(self.mssg_queue,
                                              self._update_last_ping,
                                              file_mode=DEFAULT_FILE_MODE)
            if not self.writer_thread.is_kept_alive:
                # just to be sure... with a better design, it should be unnecessary
                self.writer_thread.is_kept_alive = True
            self.writer_thread.start()

            self._update_last_ping()

            logger.info('Writer is ready!')
            logger.info('starting the listening event loop')

            while not (perf_counter() - self.last_ping) > self.timeout:
                response = dict(self.poller.poll(timeout=100))

                # on the REQ-REP line, configuration dicts are being sent
                if self.rep_socket in response:
                    self._handle_ping_request()

                if self.pull_socket in response:
                    self._handle_data_message()

            logger.info('Writer done waiting and working')

            self._dont_keep_writer_thread_alive()
            self.writer_thread.join()
            logger.info('Write thread finished')

        except:
            logger.exception('Exception in writer sub-thread', exc_info=True)

        finally:
            # remove these objects so that this method can be run again :)
            # this as a feature should probably be removed and/or not allowed
            # (preferably by design)
            self.writer_thread = None
            self.mssg_queue = None

    def _handle_data_message(self) -> None:
        """
        To be called when a poll has revealed that there is something
        in the PULL queue
        """
        logger.info('Receiving data')
        metadata = self.pull_socket.recv_json()
        data = self.pull_socket.recv_pyobj()
        mssgdict = {'metadata': metadata, 'data': data}
        self.mssg_queue.put(mssgdict)
        self._update_last_ping()

    def _handle_ping_request(self) -> None:
        """
        Handle the ping and get the configuration that sets the timeout
        """
        logger.info('Got a ping')
        mssg = self.rep_socket.recv().decode('utf-8')
        conf = json.loads(mssg)
        self.rep_socket.send(b" ")  # ping back that we are alive
        self.timeout = conf['timeout'] + 1  # to be safe
        self._update_last_ping()

    def _update_last_ping(self):
        with Lock():
            self.last_ping = perf_counter()

    def _dont_keep_writer_thread_alive(self):
        self.writer_thread.is_kept_alive = False


class WriterThread(Thread):
    """
    Implements writing data to disk that comes from a Queue that is being
    filled by the parent thread (the one that starts this thread).

    Args:
        queue: a Queue object where parent thread is going to put data
        mode: defines format of the file where the data from the queue is
            written to, defaults to GNUPLOT
    """
    def __init__(self,
                 queue: Queue,
                 msg_proc_done_clb: Callable,
                 file_mode: str=DEFAULT_FILE_MODE):
        super(WriterThread, self).__init__()

        if queue is None or not isinstance(queue, Queue):
            raise Exception('The passed queue object is not a valid queue.')
        self._queue = queue

        self._on_message_processing_finished = msg_proc_done_clb

        # parent thread can write this attribute, this thread should not
        self._is_kept_alive = True

        # instantiate an object that does the actual file writing in a
        # particular format
        self.datafile_writer = DATA_FILE_WRITERS_MAP[file_mode]()

        self.guid: str = ''

    @property
    def is_kept_alive(self):
        return self._is_kept_alive

    @is_kept_alive.setter
    def is_kept_alive(self, value):
        if isinstance(value, bool):
            self._is_kept_alive = value
        else:
            raise Exception('is_kept_alive can only be boolean')

    def run(self):
        """
        This is what the thread runs when its `start` is called.
        """
        logger.info('Off-thread writer started')

        try:
            while self.is_kept_alive:
                if not self._queue.empty():
                    mssgdict = self._queue.get()

                    guid = mssgdict['metadata']['guid']
                    chunkid = mssgdict['metadata']['chunkid']
                    datatuple = mssgdict['data']

                    if self.guid != guid:
                        logger.info("Got new guid, opening new file")
                        self.guid = guid

                        filename = os.path.join(DIR_FOR_DATAFILE, self.guid)
                        self.datafile_writer.start_new_file(filename)

                        columns: Tuple[str] = tuple(tup[0] for tup in datatuple)
                        self.datafile_writer.set_column_names(columns)

                    if chunkid == 1:
                        logger.info("Writing header")
                        self.datafile_writer.write_header()

                    # now write a line
                    logger.info(f'Writing chunk {chunkid} to GUID {guid}')
                    self.datafile_writer.write_row(datatuple)

                    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    # do we need the parent thread to
                    # know that we finished processing the message, so that
                    # it can update the last_ping?
                    # self.last_ping = perf_counter()
                    # for now implementing this via a callback that does a
                    # thread-safe update of self.last_ping
                    self._on_message_processing_finished()

                    self._queue.task_done()
        except:
            logger.exception('Exception in writer sub-thread', exc_info=True)
        finally:
            self._queue.task_done()  # is this needed?

        logger.info('Off-thread writer signing off')


def _parse_arguments():
    """
    ...

    Returns:
        a tuple that includes:
            pull_port: port number for PULL socket
            rep_port: port number for REP socket
    """
    parser = argparse.ArgumentParser(description='Be a suicidal writer')
    parser.add_argument('pull_port', metavar='pull_port', type=int,
                        help='port to pull for data')
    parser.add_argument('rep_port', metavar='rep_port', type=int,
                        help='port to reply to with status')

    args = parser.parse_args()

    pull_port = args.pull_port
    rep_port = args.rep_port

    return pull_port, rep_port


def main() -> None:
    """
    ...
    """
    pull_port, rep_port = _parse_arguments()
    writer = Writer(pull_port, rep_port)
    writer.run_message_processing_loop()


if __name__ == "__main__":
    main()
