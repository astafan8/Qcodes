import re
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
    # assert that ip addresses are not occupied

    # run the whole thing and wait for it to complete
    p = subprocess.run(list(map(str, [
        PYTHON_CMD, DIR_WITH_SUT / WRITER_FILENAME])),
                       timeout=20,
                       check=True,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       universal_newlines=True
                       )

    # test stdout and stderr
    expected_stdout = """Trying to bind pusher and req on ports 5557, 5558
Asking writer "are you alive?"
No writer found
Spawning out a writer
Spawned writer with pid ???
Asking writer "are you alive?"
Yes
Just sent message number 1
Just sent message number 2
Just sent message number 3
Just sent message number 4
Just sent message number 5
Spawner is dead"""
    actual_stdout = p.stdout
    actual_stdout_processed = re.sub(r' pid \d+', r' pid ???', actual_stdout)
    for exp, act in zip(expected_stdout.splitlines(),
                        actual_stdout_processed.splitlines()):
        assert exp == act
    assert len(expected_stdout.splitlines()) == len(
        actual_stdout_processed.splitlines())

    expected_stderr = ""
    assert expected_stderr == p.stderr

    # get the pid of writer
    writer_pid = int(re.findall(r'\d+',
                                re.findall(r' pid \d+',
                                           'zxc pid 123 asd')[0])[0])

    # assert writer is still alive


    # wait and assert that it died according to its timeout

    # read the file it created and assert its contents

    # verify that ip addresses are not occupied
