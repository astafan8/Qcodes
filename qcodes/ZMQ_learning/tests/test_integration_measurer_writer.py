import os
from subprocess import TimeoutExpired
import pickle

from qcodes.ZMQ_learning.file_writers import (
    WRITE_ROW_ARTIFICIAL_SLEEP)
from qcodes.ZMQ_learning.measurer import Measurer, WRITER_SPAWN_SLEEP_TIME
from qcodes.ZMQ_learning.common_config import DEFAULT_SUICIDE_TIMEOUT
from qcodes.ZMQ_learning.writer import (
    DIR_FOR_DATAFILE, WRITER_THREAD_WRAP_UP_TIMEOUT, DATA_FILE_WRITERS_MAP)


DEFAULT_STARTING_PORT = 6000


def try_to_wait(n_vals, measurer):
    # sleep in order to wait for writer to complete
    max_time_to_wait = WRITER_SPAWN_SLEEP_TIME \
        + n_vals * (WRITE_ROW_ARTIFICIAL_SLEEP + 0.1) \
        + WRITER_THREAD_WRAP_UP_TIMEOUT \
        + measurer.timeout
    try:
        measurer._current_writer_process.wait(timeout=max_time_to_wait)
    except TimeoutExpired as e:
        measurer._current_writer_process.kill()
        raise e


def add_weather_data(measurer):
    vals = [0, 1, 5, 6, 8]
    p_name = 'param'
    weather_data = [ ((p_name, val),) for val in vals ]
    for t in weather_data:
        measurer.add_result(t)
    return weather_data


def get_data_file_path(measurer):
    guid = measurer.guid
    writer = DATA_FILE_WRITERS_MAP[measurer._file_format]
    # find the written file
    datafile = os.path.join(
        DIR_FOR_DATAFILE,
        guid + writer._extension
    )
    assert os.path.exists(datafile), f"{datafile!r} is expected to exist!"
    return datafile


def test_measurer_initialization():
    m = Measurer(DEFAULT_STARTING_PORT)

    # asserts after initialization
    assert DEFAULT_SUICIDE_TIMEOUT == m._timeout
    assert '00000000-0000-0000-0000-000000000000' == m.guid

    m.start_run()
    assert 0 == m._current_chunk_number


def test_gnuplot_writer():
    m = Measurer(DEFAULT_STARTING_PORT,  file_format='GNUPLOT')
    m.start_run()
    weather_data = add_weather_data(m)
    try_to_wait(len(weather_data), m)
    datafile = get_data_file_path(m)

    # assert the data file contents
    vals_str = '\n'.join([str(t[0][1]) for t in weather_data])
    expected_content = f"{weather_data[0][0][0]}\n{vals_str}\n"
    file_content = open(datafile, 'r').read()
    assert expected_content == file_content


def test_pickle_writer():
    m = Measurer(DEFAULT_STARTING_PORT, file_format='PICKLE')
    m.start_run()
    weather_data = add_weather_data(m)
    try_to_wait(len(weather_data), m)
    datafile = get_data_file_path(m)
    with open(datafile, 'rb') as f:
        for t in weather_data:
            assert pickle.load(f) == t
