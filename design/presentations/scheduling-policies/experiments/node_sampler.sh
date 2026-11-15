#!/bin/bash
# Sample one node's state until killed (third campaign; run2n.sbatch starts one per node):
# every 5 s the load, the CPUs busy on the node and the CPUs this job uses there, as
# run.sbatch does; with a GPU, nvidia-smi once a second. Files: <dir>/{load,gpus,gpu}_<host>.*
# With PS_EVERY=<s> (Bridges-2), also ps of this user's processes (pid, %cpu since start, rss KiB,
# elapsed s, command) every <s> seconds, to <dir>/ps_<host>.txt.
OUT=${1:?usage: node_sampler.sh <dir>}
H=$(hostname)
CG=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup | sed 's|/step_.*||')
if [ -n "$CUDA_VISIBLE_DEVICES" ] && command -v nvidia-smi > /dev/null; then
  nvidia-smi --query-gpu=index,pci.bus_id,name,memory.total --format=csv,noheader > "$OUT/gpus_$H.txt"
  nvidia-smi --query-gpu=timestamp,index,utilization.gpu,memory.used --format=csv,noheader,nounits \
    -lms 1000 > "$OUT/gpu_$H.csv" 2>/dev/null &
fi
if [ -n "$PS_EVERY" ]; then  # Bridges-2: per-process CPU of this user's processes every PS_EVERY s
  (while true; do
     echo "# $(date +%s.%N)"
     ps -u "$USER" -o pid=,pcpu=,rss=,etimes=,comm=,args= --sort=-pcpu | cut -c1-200 | head -200
     sleep "$PS_EVERY"
   done) > "$OUT/ps_$H.txt" 2>/dev/null &
fi
trap 'kill $(jobs -p) 2>/dev/null; exit 0' TERM INT HUP
prev_t=
while true; do
  read -r _ u n s i w q sq st _ < /proc/stat
  tot=$((u + n + s + i + w + q + sq + st)); busy=$((tot - i - w))
  mine=$(awk '/^usage_usec/{print $2}' "$CG/cpu.stat" 2>/dev/null)
  t=$(date +%s.%N)
  if [ -n "$prev_t" ]; then
    awk -v t="$t" -v pt="$prev_t" -v b="$busy" -v pb="$prev_busy" -v to="$tot" -v pto="$prev_tot" \
        -v m="$mine" -v pm="$prev_mine" -v ncpu="$(nproc --all)" -v la="$(cut -d' ' -f1 /proc/loadavg)" \
      'BEGIN { node = (b - pb) / (to - pto) * ncpu; me = (m - pm) / ((t - pt) * 1e6);
               printf "%.1f,%s,%.2f,%.2f\n", t, la, node, me }' >> "$OUT/load_$H.csv"
  fi
  prev_t=$t; prev_busy=$busy; prev_tot=$tot; prev_mine=$mine
  sleep 5 &
  wait $!
done
