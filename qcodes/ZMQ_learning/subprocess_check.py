import subprocess

cmd1 = ["python", "-c", "import sys;print(sys.base_prefix)"]

output = subprocess.check_output(cmd1)

print('Got output')
print(output)