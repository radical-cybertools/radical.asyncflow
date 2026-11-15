# Design notes and experiment records

Working notes behind AsyncFlow's scheduling policies and the Dragon backends, written between
October and November 2026. They record what was tried, on which machines, and what came out. They
are not user documentation, and some describe local branches that later became pull requests.

## Scheduling policies

- [policy-layer.md](policy-layer.md): the design of the policy layer.
- [policy-s1-results.md](policy-s1-results.md): stage S1, resource vectors and learned memory, on a
  simulated trace.
- [pr-ordering-policies.md](pr-ordering-policies.md), [pr-resource-policy.md](pr-resource-policy.md),
  [pr-tickless-on-hot-path.md](pr-tickless-on-hot-path.md): drafts of the pull request descriptions.
- Experiment campaigns, in order:
  [rust-experiments.md](rust-experiments.md), [rust-experiments-2.md](rust-experiments-2.md),
  [rust-experiments-3.md](rust-experiments-3.md), [rust-experiments-4.md](rust-experiments-4.md)
  (a Slurm cluster whose nodes are haskell, julia and fortran), and
  [bridges2-experiments.md](bridges2-experiments.md) (PSC Bridges-2).
- [presentations/scheduling-policies/](presentations/scheduling-policies/): the slide deck
  (LaTeX Beamer), the experiment scripts (`experiments/`), and the summaries and charts
  (`results/`).

## Dragon

- [dragon-rust-findings.md](dragon-rust-findings.md): Dragon behaviour found on the rust cluster.
- [mixed-kinds.md](mixed-kinds.md), [mpi-check.md](mpi-check.md), [mpi-gpu.md](mpi-gpu.md): checks of
  mixed task kinds, MPI jobs and MPI with GPUs, with their scripts in the folders of the same name.

## What is not here

Raw run data, per-run diagnostics and Dragon debug logs are kept outside the repository. Paths in the
notes that point into `results/*/raw/` or `results/*/diag/` refer to them. Other users' Slurm names
and other groups' partition names are replaced with `userA`–`userD`, `group1` and `group2`.
