#!/usr/bin/env python3
"""Synthetic GPU task for E2: hold a set amount of GPU memory and compute for a set time.

It uses only the CUDA driver (libcuda through ctypes) and a hand-written PTX kernel, so it
needs neither the CUDA toolkit nor PyTorch or CuPy, none of which is installed on rust.

The task:
1. creates a CUDA context on visible device 0 (blocking sync, so waiting costs no CPU);
2. allocates ``--mem-gib`` GiB in one block and writes it once (a CUDA OOM exits with 3);
3. runs ``--secs`` seconds in periods of ``--period`` s: a GPU burst of ``--gpu-frac`` of the
   period, measured on an idle GPU, then sleeps for the rest. On a shared GPU the bursts
   take longer, so the task takes longer, as a real tool would;
4. writes a JSON record to ``--out`` and exits 0.

Exit codes: 0 done, 3 CUDA out of memory, 4 any other CUDA error.

``--calibrate`` prints the kernel rate (iterations per ms) and the memory one context takes.
``--iters-per-ms-by-gpu "Ada=79000,Blackwell=..."`` picks the rate by the device's name (the
first key it contains), so a task computes for the same time on either GPU model.
"""

import argparse
import ctypes
import json
import os
import socket
import sys
import time

PTX = rb"""
.version 6.0
.target sm_50
.address_size 64

.visible .entry spin(.param .u64 out, .param .u32 iters)
{
    .reg .pred %p<2>;
    .reg .f32 %f<2>;
    .reg .b32 %r<8>;
    .reg .b64 %rd<5>;

    ld.param.u64 %rd1, [out];
    ld.param.u32 %r1, [iters];
    mov.u32 %r2, %ctaid.x;
    mov.u32 %r3, %ntid.x;
    mov.u32 %r4, %tid.x;
    mad.lo.s32 %r5, %r2, %r3, %r4;
    cvt.rn.f32.u32 %f1, %r5;
    mov.u32 %r6, 0;
LOOP:
    fma.rn.f32 %f1, %f1, 0f3F7FFFEF, 0f3F800000;
    add.s32 %r6, %r6, 1;
    setp.lt.u32 %p1, %r6, %r1;
    @%p1 bra LOOP;
    cvta.to.global.u64 %rd2, %rd1;
    mul.wide.u32 %rd3, %r5, 4;
    add.s64 %rd4, %rd2, %rd3;
    st.global.f32 [%rd4], %f1;
    ret;
}
"""

CUDA_ERROR_OUT_OF_MEMORY = 2
CU_CTX_SCHED_BLOCKING_SYNC = 0x04
CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT = 16
BLOCK = 256
BLOCKS_PER_SM = 8
GIB = 1 << 30


class CudaError(Exception):
    def __init__(self, call, code, name):
        super().__init__(f"{call}: {name} ({code})")
        self.call, self.code, self.name = call, code, name


# The EESSI compat-layer Python finds libcuda by name on haskell (EESSI host_injections), but
# not on fortran (third campaign, job 24790). There it loads from the driver's directory, and
# so must libcuda's PTX JIT compiler, which libcuda opens by name when it loads the kernel
# (CUDA_ERROR_JIT_COMPILER_NOT_FOUND otherwise, job 24792): loaded first, globally, it is found.
DRIVER_DIR = "/usr/lib/x86_64-linux-gnu"


class Cuda:
    def __init__(self):
        try:
            self.lib = ctypes.CDLL("libcuda.so.1")
        except OSError:
            jit = os.path.join(DRIVER_DIR, "libnvidia-ptxjitcompiler.so.1")
            if os.path.exists(jit):
                ctypes.CDLL(jit, mode=ctypes.RTLD_GLOBAL)
            self.lib = ctypes.CDLL(os.path.join(DRIVER_DIR, "libcuda.so.1"))

    def __call__(self, fn, *args):
        code = getattr(self.lib, fn)(*args)
        if code != 0:
            name = ctypes.c_char_p()
            self.lib.cuGetErrorName(code, ctypes.byref(name))
            raise CudaError(fn, code, (name.value or b"?").decode())


def open_device(cu):
    cu("cuInit", 0)
    dev = ctypes.c_int()
    cu("cuDeviceGet", ctypes.byref(dev), 0)
    bus = ctypes.create_string_buffer(32)
    cu("cuDeviceGetPCIBusId", bus, 32, dev)
    sms = ctypes.c_int()
    cu("cuDeviceGetAttribute", ctypes.byref(sms), CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT, dev)
    ctx = ctypes.c_void_p()
    cu("cuCtxCreate_v2", ctypes.byref(ctx), CU_CTX_SCHED_BLOCKING_SYNC, dev)
    free, total = ctypes.c_size_t(), ctypes.c_size_t()
    cu("cuMemGetInfo_v2", ctypes.byref(free), ctypes.byref(total))
    return ctx, bus.value.decode(), sms.value, free.value, total.value


def device_name(cu):
    name = ctypes.create_string_buffer(128)
    dev = ctypes.c_int()
    cu("cuDeviceGet", ctypes.byref(dev), 0)
    cu("cuDeviceGetName", name, 128, dev)
    return name.value.decode()


def rate_for(name, by_gpu, default):
    """The ``key=rate`` entry of ``by_gpu`` whose key is in the device name, else ``default``."""
    for item in filter(None, (by_gpu or "").split(",")):
        key, rate = item.split("=")
        if key.strip() in name:
            return float(rate)
    return default


class Kernel:
    def __init__(self, cu, sms):
        self.cu = cu
        mod = ctypes.c_void_p()
        cu("cuModuleLoadData", ctypes.byref(mod), ctypes.c_char_p(PTX))
        self.fn = ctypes.c_void_p()
        cu("cuModuleGetFunction", ctypes.byref(self.fn), mod, b"spin")
        self.grid = sms * BLOCKS_PER_SM
        self.out = ctypes.c_uint64()
        cu("cuMemAlloc_v2", ctypes.byref(self.out), ctypes.c_size_t(self.grid * BLOCK * 4))

    def run(self, iters):
        """Launch one kernel of ``iters`` iterations per thread and wait for it."""
        out = ctypes.c_uint64(self.out.value)
        n = ctypes.c_uint32(max(1, int(iters)))
        params = (ctypes.c_void_p * 2)(
            ctypes.cast(ctypes.byref(out), ctypes.c_void_p),
            ctypes.cast(ctypes.byref(n), ctypes.c_void_p),
        )
        self.cu("cuLaunchKernel", self.fn, self.grid, 1, 1, BLOCK, 1, 1, 0, None, params, None)
        self.cu("cuCtxSynchronize")


def calibrate():
    cu = Cuda()
    t0 = time.time()
    _, bus, sms, free, total = open_device(cu)
    t_ctx = time.time() - t0
    k = Kernel(cu, sms)
    k.run(1000)  # JIT and warm-up
    rates = []
    for iters in (20000, 50000, 100000, 200000):
        t = time.perf_counter()
        k.run(iters)
        rates.append(iters / ((time.perf_counter() - t) * 1e3))
    print(
        json.dumps(
            {
                "bus": bus,
                "name": device_name(cu),
                "sms": sms,
                "ctx_create_s": round(t_ctx, 3),
                "total_gib": total / GIB,
                "free_after_ctx_gib": free / GIB,
                "iters_per_ms": [round(r, 1) for r in rates],
                "cvd": os.environ.get("CUDA_VISIBLE_DEVICES"),
            }
        )
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mem-gib", type=float, default=1.0)
    ap.add_argument("--secs", type=float, default=10.0)
    ap.add_argument("--gpu-frac", type=float, default=0.25)
    ap.add_argument("--period", type=float, default=2.0)
    ap.add_argument("--iters-per-ms", type=float, default=0.0)
    ap.add_argument("--iters-per-ms-by-gpu", default="")
    ap.add_argument("--out", default=None)
    ap.add_argument("--tag", default="")
    ap.add_argument("--calibrate", action="store_true")
    a = ap.parse_args()
    if a.calibrate:
        calibrate()
        return 0

    rec = {
        "tag": a.tag,
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "cvd": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "mem_gib": a.mem_gib,
        "secs": a.secs,
        "gpu_frac": a.gpu_frac,
        "t_start": time.time(),
    }
    status, rc = "done", 0
    cu = Cuda()
    try:
        _, rec["bus"], sms, free, total = open_device(cu)
        rec["t_ctx"] = time.time()
        rec["free_gib_at_start"] = free / GIB
        rec["total_gib"] = total / GIB
        rec["gpu_name"] = device_name(cu)
        rec["iters_per_ms"] = rate_for(rec["gpu_name"], a.iters_per_ms_by_gpu, a.iters_per_ms)
        k = Kernel(cu, sms)
        buf = ctypes.c_uint64()
        cu("cuMemAlloc_v2", ctypes.byref(buf), ctypes.c_size_t(int(a.mem_gib * GIB)))
        cu("cuMemsetD8_v2", buf, 0, ctypes.c_size_t(int(a.mem_gib * GIB)))
        rec["t_alloc"] = time.time()
        burst_ms = a.gpu_frac * a.period * 1e3
        gpu_s = 0.0
        for _ in range(max(1, round(a.secs / a.period))):
            t = time.perf_counter()
            if burst_ms > 0:
                k.run(burst_ms * rec["iters_per_ms"])
            dt = time.perf_counter() - t
            gpu_s += dt
            time.sleep(max(0.0, (1 - a.gpu_frac) * a.period))
        rec["gpu_burst_s"] = gpu_s
    except CudaError as e:
        status = "oom" if e.code == CUDA_ERROR_OUT_OF_MEMORY else "cuda_error"
        rc = 3 if status == "oom" else 4
        rec["error"] = str(e)
    rec["t_end"] = time.time()
    rec["status"] = status
    if a.out:
        tmp = a.out + ".tmp"
        with open(tmp, "w") as f:
            json.dump(rec, f)
        os.replace(tmp, a.out)
    print(json.dumps(rec))
    return rc


if __name__ == "__main__":
    sys.exit(main())
