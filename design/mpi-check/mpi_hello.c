/* One MPI job's view of itself: each rank writes rank-<r>.txt in <outdir> with the size it
   sees and an Allreduce of rank+1. Without a working PMI every rank starts alone (rank 0 of
   1), so it exits 4 unless the size is the <expected> it was given. With a third argument
   "fail", rank 1 exits 7 right after MPI_Init and the others wait for it in the Allreduce. */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: mpi_hello <outdir> <expected> [fail]\n"); return 2; }
    int expected = atoi(argv[2]);
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if (argc > 3 && strcmp(argv[3], "fail") == 0 && rank == 1) _exit(7);
    char host[256];
    gethostname(host, sizeof host);
    long v = rank + 1, sum = 0;
    MPI_Allreduce(&v, &sum, 1, MPI_LONG, MPI_SUM, MPI_COMM_WORLD);
    char path[1024];
    snprintf(path, sizeof path, "%s/rank-%d-pid-%d.txt", argv[1], rank, (int)getpid());
    FILE *f = fopen(path, "w");
    if (f) {
        fprintf(f, "rank %d of %d on %s sum %ld (want %ld)\n", rank, size, host, sum,
                (long)size * (size + 1) / 2);
        fclose(f);
    }
    MPI_Finalize();
    if (size != expected) return 4;
    return sum == (long)size * (size + 1) / 2 ? 0 : 3;
}
