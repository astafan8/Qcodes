import subprocess
from pathlib import PurePath
from time import sleep
import os

import pytest


DIR_WITH_SUT = PurePath(os.path.realpath(__file__)).parent.parent
WRITER_FILENAME = 'push_pull_handshake_spawner_POPEN.py'
SPAWNER_FILENAME = 'push_pull_handshake_suicidal_writer.py'

PYTHON_CMD = 'python'


def test_writer_dying():
    spawner_proc = None
    writer_proc = None
    try:
        spawner_proc = subprocess.Popen(list(map(str, [PYTHON_CMD,
                                                       DIR_WITH_SUT /
                                        WRITER_FILENAME])))
        sleep(10)  # s

        if spawner_proc.poll() is not None:
            pytest.fail(msg='Spawner died prematurely...')

        writer_proc = subprocess.Popen(list(map(str, [PYTHON_CMD,
                                                       DIR_WITH_SUT /
                                       SPAWNER_FILENAME, 5557, 5558])))
        sleep(3)  # s
    except:
        for proc in [spawner_proc, writer_proc]:
            if proc is not None:
                proc.kill()
        raise
    else:
        for proc in [spawner_proc, writer_proc]:
            if proc is not None:
                if proc.poll() is None:
                    proc.wait(timeout=20)  # s
    finally:
        for proc in [spawner_proc, writer_proc]:
            if proc is not None:
                msg_stderr = proc.stderr
                assert 0 == proc.returncode, msg_stderr


def test_writer():
    subprocess.run(list(map(str, [
        PYTHON_CMD, DIR_WITH_SUT / SPAWNER_FILENAME, 5557, 5558])), timeout=20)


def test_spawner():
    subprocess.run(list(map(str, [
        PYTHON_CMD, DIR_WITH_SUT / WRITER_FILENAME])), timeout=20)
