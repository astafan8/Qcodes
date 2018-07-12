"""
This test should print:

Starting experimental run with id: 2
p_a is 0.0, p_b is 20.0, p_c is 200.0; sum is 220.0
p_a is 2.0, p_b is 36.0, p_c is 560.0; sum is 598.0
p_a is 4.0, p_b is 52.0, p_c is 920.0; sum is 976.0
p_a is 6.0, p_b is 68.0, p_c is 1280.0; sum is 1354.0
p_a is 8.0, p_b is 84.0, p_c is 1640.0; sum is 1732.0
p_a is 10.0, p_b is 100.0, p_c is 2000.0; sum is 2110.0
data id =  2

"""

import qcodes

import sys
sys.path.append(r"D:\Code\Qcodes")


import numpy as np

from qcodes.sweep import getter, \
    SweepMeasurement, nest, \
    parallel_sweep

from qcodes import new_experiment, Station


p_a = qcodes.ManualParameter('p_a')
p_b = qcodes.ManualParameter('p_b')
p_c = qcodes.ManualParameter('p_c')

@getter([
    ("p_sum", "a.u.")
])
def measure_sum():
    val = p_a.get() + p_b.get() + p_c.get()
    print(f"p_a is {p_a.get()}, p_b is {p_b.get()}, p_c is {p_c.get()}; sum is {val}")
    return val,


sweep_object = nest(
    parallel_sweep(
        (p_a, np.linspace(0, 10, 6)),
        (p_b, np.linspace(20, 100, 6)),
        (p_c, np.linspace(200, 2000, 6))
    ),
    measure_sum
)


exp = new_experiment('test_parallel_sweep', sample_name='qwe')
meas = SweepMeasurement(station=Station())

# meas
meas.register_sweep(sweep_object)

# perform exp
with meas.run() as datasaver:
    # add other subscriber for the plottr (see the xample notebook)
    for data in sweep_object:
        datasaver.add_result(*data.items())

dataid = datasaver.run_id  # convenient to have for plotting

print("data id = ", dataid)