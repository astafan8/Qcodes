"""
This module contains code used for benchmarking data saving speed of the
database used under the QCoDeS dataset.
"""
import shutil
import tempfile
import os
import time

import numpy as np

import qcodes
from qcodes import ManualParameter, load_by_id
from qcodes.dataset.measurements import Measurement
from qcodes.dataset.experiment_container import new_experiment
from qcodes.dataset.database import initialise_database


class Adding5Params:
    """
    This benchmark measures how much time it takes to save a certain amount of
    data to the experiment database. Parametrization is used to alter how much
    data is being saved.
    """

    # For this benchmark, we can not reuse what is being set up in setup method,
    # hence the number of iterations is limited to 1
    number = 1

    # In order to get more stable result, the following number of repeats is
    # used; note that repeats include setting up and tearing down
    repeat = 8

    # These are the parameters of this benchmark: n_values to write per
    # add_results call, n_times to call add_results
    # Dictionary of values is used instead of tuple of lists, because in the
    # latter case asv will run the benchmark for all the combinations of the
    # values
    params = [
        {'n_values': 10000, 'n_times': 2, 'paramtype': 'array'},
        {'n_values': 100, 'n_times': 200, 'paramtype': 'array'},
        {'n_values': 10000, 'n_times': 2, 'paramtype': 'numeric'},
        {'n_values': 100, 'n_times': 200, 'paramtype': 'numeric'},
    ]

    # we are less interested in the cpu time used and more interested in
    # the wall clock time used to insert the data so use a timer that measures
    # wallclock time
    timer = time.perf_counter

    def __init__(self):
        self.parameters = list()
        self.values = list()
        self.experiment = None
        self.runner = None
        self.datasaver = None
        self.tmpdir = None

    def setup(self, bench_param):
        # Init DB
        self.tmpdir = tempfile.mkdtemp()
        qcodes.config["core"]["db_location"] = os.path.join(self.tmpdir,
                                                            'temp.db')
        qcodes.config["core"]["db_debug"] = False
        initialise_database()

        # Create experiment
        self.experiment = new_experiment("test-experiment",
                                         sample_name="test-sample")

        # Create measurement
        meas = Measurement(self.experiment)

        x1 = ManualParameter('x1')
        x2 = ManualParameter('x2')
        x3 = ManualParameter('x3')
        y1 = ManualParameter('y1')
        y2 = ManualParameter('y2')

        meas.register_parameter(x1, paramtype=bench_param['paramtype'])
        meas.register_parameter(x2, paramtype=bench_param['paramtype'])
        meas.register_parameter(x3, paramtype=bench_param['paramtype'])
        meas.register_parameter(y1, setpoints=[x1, x2, x3],
                                paramtype=bench_param['paramtype'])
        meas.register_parameter(y2, setpoints=[x1, x2, x3],
                                paramtype=bench_param['paramtype'])

        self.parameters = [x1, x2, x3, y1, y2]

        # Create the Runner context manager
        self.runner = meas.run()

        # Enter Runner and create DataSaver
        self.datasaver = self.runner.__enter__()

        # Create values for parameters
        for _ in range(len(self.parameters)):
            self.values.append(np.random.rand(bench_param['n_values']))

    def teardown(self, bench_param):
        # Exit runner context manager
        if self.runner:
            self.runner.__exit__(None, None, None)
            self.runner = None
            self.datasaver = None

        # Close DB connection
        if self.experiment:
            self.experiment.conn.close()
            self.experiment = None

        # Remove tmpdir with database
        if self.tmpdir:
            shutil.rmtree(self.tmpdir)
            self.tmpdir = None

        self.parameters = list()
        self.values = list()

    def time_test(self, bench_param):
        """Adding data for 5 parameters"""
        for _ in range(bench_param['n_times']):
            self.datasaver.add_result(
                (self.parameters[0], self.values[0]),
                (self.parameters[1], self.values[1]),
                (self.parameters[2], self.values[2]),
                (self.parameters[3], self.values[3]),
                (self.parameters[4], self.values[4])
            )
        # force writing to database so that it is written before we exit
        # the datasaver context manager
        self.datasaver.flush_data_to_database()


class DataSetGetData:
    """
    This benchmark measures how much time it takes to load a certain amount of
    data from the experiment database by using the DataSet.get_data method.
    Parametrization is used to alter how much data is being loaded.
    """

    # For this benchmark, we can not reuse what is being set up in setup method,
    # hence the number of iterations is limited to 1
    number = 1

    # In order to get more stable result, the following number of repeats is
    # used; note that repeats include setting up and tearing down
    repeat = 4

    # These are the parameters of this benchmark: n_values_per_param is
    # written to the database using type paramtype
    # Dictionary of values is used instead of tuple of lists, because in the
    # latter case asv will run the benchmark for all the combinations of the
    # values
    params = [
        {'n_values_per_param': 20, 'paramtype': 'array'},
        {'n_values_per_param': 20, 'paramtype': 'numeric'},
    ]

    # we are less interested in the cpu time used and more interested in
    # the wall clock time used to insert the data so use a timer that measures
    # wallclock time
    timer = time.perf_counter

    def __init__(self):
        self.parameters = list()
        self.values = list()
        self.tmpdir = None
        self.run_id = None

    def setup(self, bench_param):
        # Init DB
        self.tmpdir = tempfile.mkdtemp()
        qcodes.config["core"]["db_location"] = os.path.join(self.tmpdir,
                                                            'temp.db')
        qcodes.config["core"]["db_debug"] = False
        initialise_database()

        # Create experiment
        experiment = new_experiment("get_data_from_db",
                                    sample_name="some_sample")

        # Create measurement
        meas = Measurement(experiment)

        x1 = ManualParameter('x1')
        x2 = ManualParameter('x2')
        x3 = ManualParameter('x3')
        y1 = ManualParameter('y1')

        meas.register_parameter(x1, paramtype=bench_param['paramtype'])
        meas.register_parameter(x2, paramtype=bench_param['paramtype'])
        meas.register_parameter(x3, paramtype=bench_param['paramtype'])
        meas.register_parameter(y1, setpoints=[x1, x2, x3],
                                paramtype=bench_param['paramtype'])

        self.parameters = [x1, x2, x3, y1]

        # Write some data to the database
        with meas.run() as datasaver:
            x1_vals_mtx, x2_vals_mtx, x3_vals_mtx, y1_vals_mtx = np.meshgrid(
                np.random.rand(bench_param['n_values_per_param']),
                np.random.rand(bench_param['n_values_per_param']),
                np.random.rand(bench_param['n_values_per_param']),
                np.random.rand(bench_param['n_values_per_param'])
            )
            x1_vals = np.reshape(x1_vals_mtx, -1)
            x2_vals = np.reshape(x2_vals_mtx, -1)
            x3_vals = np.reshape(x3_vals_mtx, -1)
            y1_vals = np.reshape(y1_vals_mtx, -1)
            datasaver.add_result((x1, x1_vals), (x2, x2_vals), (x3, x3_vals),
                                 (y1, y1_vals))

        self.run_id = datasaver.run_id

    def teardown(self, bench_param):
        # Remove tmpdir with database
        if self.tmpdir:
            shutil.rmtree(self.tmpdir)
            self.tmpdir = None

        self.parameters = list()
        self.values = list()
        self.run_id = None

    def time_test_per_param(self, bench_param):
        """Loading data using get_data"""
        dataset = load_by_id(self.run_id)

        for p in self.parameters:
            _ = dataset.get_data(p.name)

        dataset.conn.close()

    def time_test_all_params(self, bench_param):
        """Loading data using get_data"""
        dataset = load_by_id(self.run_id)

        _ = dataset.get_data(*[p.name for p in self.parameters])

        dataset.conn.close()
