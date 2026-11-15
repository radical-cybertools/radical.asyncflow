#!/bin/bash
# run_case.sh <pg|batch> <pmi> <ranks>: one Dragon runtime on this host, in case-<driver>-<pmi>-<n>/,
# then what it left behind, which it ends: only processes of this user whose working directory
# is that case directory, and only the /dev/shm files Dragon's exit warning names for this run.
set -u
cd "$(dirname "$0")"
here=$PWD drv=$1 pmi=$2 n=$3 case=$PWD/case-$1-$2-$3
rm -rf "$case" && mkdir -p "$case" && touch "$case/.start" || exit 1
t0=$(date +%s.%N)
(
  cd "$case" || exit 1
  PATH=$HOME/tmp/radical-kits-venv/bin:$PATH timeout --signal=INT --kill-after=30 240 \
    dragon "$here/${drv}_check.py" "$here/mpi_hello" "$case/out" "$n" "$pmi" >run.out 2>run.err
)
rc=$?
echo "== $(basename "$case"): dragon rc=$rc in $(python3 -c "print(round($(date +%s.%N) - $t0, 1))") s"
grep -a RESULT "$case/run.out" || {
  echo "-- no RESULT:"
  grep -a -v -E "^DD" "$case/run.out" | grep -a -E "Error|error|Abnormal|exception|Traceback" | sort -u | head -6
  tail -4 "$case/run.err"
}
echo "rank files: $(ls "$case/out" 2>/dev/null | wc -l)"
cat "$case"/out/* 2>/dev/null | sed -E 's/ on three//' | sort | uniq -c | head -6
mine() { for p in $(pgrep -u "$USER"); do
  case $(readlink "/proc/$p/cwd" 2>/dev/null) in "$case" | "$case"/*) echo "$p" ;; esac; done; }
left=$(mine | xargs)
if [[ -n $left ]]; then
  echo "left: $(ps -o comm= -p "${left// /,}" | sort | uniq -c | xargs)"
  kill -TERM $left 2>/dev/null
  for _ in 1 2 3 4 5; do [[ -z $(mine) ]] && break; timeout 1 tail -f /dev/null; done
  [[ -n $(mine) ]] && kill -KILL $(mine) 2>/dev/null
  echo "after cleanup: $(mine | wc -l) left"
fi
shm=$(sed -n "/leftover files in \/dev\/shm/,\$p" "$case/run.out" | tr ' ' '\n' | grep -E '^DD' | sort -u)
for f in $shm; do [[ -O /dev/shm/$f ]] && rm -f -- "/dev/shm/$f"; done
[[ -n $shm ]] && echo "removed $(wc -w <<<"$shm") /dev/shm file(s) named in Dragon's warning"
# Open MPI leaves a 16 MiB sm_segment file per job it starts without mpirun.
n_seg=$(find /dev/shm -maxdepth 1 -user "$USER" -name "sm_segment.*" -newer "$case/.start" -print -delete | wc -l)
((n_seg > 0)) && echo "removed $n_seg Open MPI sm_segment file(s) from this case"
true
