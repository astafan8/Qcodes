import os
from subprocess import TimeoutExpired
import pickle

from qcodes.ZMQ_learning.file_writers import (
    GnuplotWriter, PickleWriter, WRITE_ROW_ARTIFICIAL_SLEEP)
from qcodes.ZMQ_learning.measurer import Measurer, WRITER_SPAWN_SLEEP_TIME
from qcodes.ZMQ_learning.common_config import DEFAULT_SUICIDE_TIMEOUT
from qcodes.ZMQ_learning.writer import (
    DIR_FOR_DATAFILE, WRITER_THREAD_WRAP_UP_TIMEOUT, DATA_FILE_WRITERS_MAP)
import qcodes.ZMQ_learning.writer as writer_module


DEFAULT_STARTING_PORT = 6000


def test_good_weather():
    for writer_name, writer in DATA_FILE_WRITERS_MAP.items():
        m = Measurer(DEFAULT_STARTING_PORT, file_format=writer_name)

        # asserts after initialization
        assert DEFAULT_SUICIDE_TIMEOUT == m._timeout
        assert '00000000-0000-0000-0000-000000000000' == m.guid

        # start run, after this we are supposed to call 'add_result'
        m.start_run()
        guid = m.guid
        assert 0 == m._current_chunk_number

        # add some results in a loop
        vals = [0, 1, 5, 6, 8]
        p_name = 'param'
        for val in vals:
            m.add_result(((p_name, val),))

        # sleep in order to wait for writer to complete
        max_time_to_wait = WRITER_SPAWN_SLEEP_TIME \
                        + len(vals) * (WRITE_ROW_ARTIFICIAL_SLEEP + 0.1) \
                        + WRITER_THREAD_WRAP_UP_TIMEOUT \
                        + m.timeout
        try:
            m._current_writer_process.wait(timeout=max_time_to_wait)
        except TimeoutExpired as e:
            m._current_writer_process.kill()
            raise e

        # find the written file
        datafile = os.path.join(
            DIR_FOR_DATAFILE,
            guid + writer._extension
        )
        assert os.path.exists(datafile), f"{datafile!r} is expected to exist!"

        if writer is GnuplotWriter:
            # assert the data file contents
            vals_str = '\n'.join([str(val) for val in vals])
            expected_content = f"{p_name}\n{vals_str}\n"
            file_content = open(datafile, 'r').read()
            assert expected_content == file_content
        elif writer is PickleWriter:
            with open(datafile, 'rb') as f:
                for v in vals:
                    assert pickle.load(f) == (('param', v),)
