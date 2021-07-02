"""
Test suite for utils.threading.*
"""
import threading
from collections import defaultdict
from typing import Any

import pytest

from qcodes.instrument.parameter import Parameter, ParamRawDataType
from qcodes.utils.threading import call_params_threaded

from .instrument_mocks import DummyInstrument


class ParameterWithThreadKnowledge(Parameter):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def get_raw(self) -> ParamRawDataType:
        return threading.get_ident()


@pytest.fixture(name='dummy_1', scope='function')
def _dummy_dac1():
    instrument = DummyInstrument(
        name='dummy_1', gates=['ch1'])

    instrument.add_parameter(name='voltage_1',
                             parameter_class=ParameterWithThreadKnowledge)

    instrument.add_parameter(name='voltage_2',
                             parameter_class=ParameterWithThreadKnowledge)

    try:
        yield instrument
    finally:
        instrument.close()


@pytest.fixture(name='dummy_2', scope='function')
def _dummy_dac2():
    instrument = DummyInstrument(
        name='dummy_2', gates=['ch1'])

    instrument.add_parameter(name='voltage_1',
                             parameter_class=ParameterWithThreadKnowledge)

    instrument.add_parameter(name='voltage_2',
                             parameter_class=ParameterWithThreadKnowledge)

    try:
        yield instrument
    finally:
        instrument.close()


def test_call_params_threaded(dummy_1, dummy_2):

    params_output = call_params_threaded((dummy_1.voltage_1,
                                          dummy_1.voltage_2,
                                          dummy_2.voltage_1,
                                          dummy_2.voltage_2))

    params_per_thread_id = defaultdict(set)
    for param, thread_id in params_output:
        params_per_thread_id[thread_id].add(param)
    assert len(params_per_thread_id) == 2
    expected_params_per_thread = {
        frozenset([dummy_1.voltage_1, dummy_1.voltage_2]),
        frozenset([dummy_2.voltage_1, dummy_2.voltage_2])
    }
    assert {
        frozenset(value) for value in params_per_thread_id.values()
    } == expected_params_per_thread