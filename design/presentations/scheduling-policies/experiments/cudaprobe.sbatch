#!/bin/bash
# Where is libcuda.so.1 on each node, and can the EESSI compat-layer Python load it (third campaign)?
source /home/mturilli/tmp/dragon-rust/venv/bin/activate
for g in 0 1; do
  timeout -k 5 60 srun --overlap --het-group=$g -n1 bash -c "echo == \$(hostname); cat /etc/os-release | grep PRETTY; ls -l /usr/lib/x86_64-linux-gnu/libcuda.so* /usr/lib64/libcuda.so* /usr/lib/libcuda.so* 2>&1 | head; /sbin/ldconfig -p | grep libcuda; echo LD_LIBRARY_PATH=\$LD_LIBRARY_PATH;
    python -c \"import ctypes, sys; print(sys.executable)
try:
    ctypes.CDLL(\\\"libcuda.so.1\\\"); print(\\\"libcuda.so.1: ok\\\")
except OSError as e:
    print(\\\"libcuda.so.1:\\\", e)
for p in (\\\"/usr/lib/x86_64-linux-gnu/libcuda.so.1\\\", \\\"/usr/lib64/libcuda.so.1\\\"):
    try:
        ctypes.CDLL(p); print(p, \\\"ok\\\")
    except OSError as e:
        print(p, e)\""
done
