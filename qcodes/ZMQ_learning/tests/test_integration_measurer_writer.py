import os

from qcodes.ZMQ_learning.file_writers import GnuplotWriter, \
    WRITE_ROW_ARTIFICIAL_SLEEP
from qcodes.ZMQ_learning.measurer import Measurer
from qcodes.ZMQ_learning.common_config import DEFAULT_SUICIDE_TIMEOUT
from qcodes.ZMQ_learning.writer import DIR_FOR_DATAFILE


DEFAULT_STARTING_PORT = 6000


def test_good_weather():
    m = Measurer(DEFAULT_STARTING_PORT)

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
    # for some reason just the artificial sleep time is not enough,
    # and even adding a single value of timeout does not help,
    # hence the factor of 2 for the timeout value
    max_time_to_wait = len(vals) * WRITE_ROW_ARTIFICIAL_SLEEP * 1.1 \
                       + m.timeout * 2
    m._current_writer_process.wait(timeout=max_time_to_wait)

    # find the written file
    datafile = os.path.join(
        DIR_FOR_DATAFILE,
        guid + GnuplotWriter._extension
    )
    assert os.path.exists(datafile), f"{datafile!r} is expected to exist!"

    # assert the data file contents
    vals_str = '\n'.join([str(val) for val in vals])
    expected_content = f"{p_name}\n{vals_str}\n"
    file_content = open(datafile, 'r').read()
    assert expected_content == file_content
