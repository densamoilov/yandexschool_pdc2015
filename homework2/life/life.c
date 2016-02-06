/*
* Samoylov Denis pdc_shad 2015
* life.cpp: Conway's Game of life MPI version
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <mpi.h>

#define ALIVE 'X'
#define DEAD '.'

int toindex(int row, int col, int all_row, int all_col) 
{
    if (row < 0) {
        row = row + all_row;
    } else if (row >= all_row) {
        row = row - all_row;
    }
    if (col < 0) {
        col = col + all_col;
    } else if (col >= all_col) {
        col = col - all_col;
    }
    return row * all_col + col;
}

void printgrid(char* grid, char* buf, FILE* f, int N) 
{
    for (int i = 0; i < N; ++i) {
        strncpy(buf, grid + i * N, N);
        buf[N] = 0;
        fprintf(f, "%s\n", buf);
    }
}


int main(int argc, char** argv) 
{
    int rank, nprocesses;
    
    MPI_Init(&argc, &argv);
    double totaltime = MPI_Wtime(); 
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocesses);
    
    if (argc != 5) {
        if (rank == 0) {
            fprintf(stderr, "Usage: %s <Grid size (N)> <input_file> <iterations> <output_file>\n", argv[0]);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // grid size
    int N = atoi(argv[1]);
    if (N < nprocesses || (N % nprocesses) != 0) {
        if (rank == 0) {
            fprintf(stderr, "The grid size must be a multiple of the number of processes\n");
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    int iterations = atoi(argv[3]);
    int rows_per_process = N / nprocesses;

    char* grid = NULL;
    // Plus two rows for shadow points
    char* grid_local = (char*) malloc((rows_per_process * N + 2 * N) * sizeof(char));
    char* grid_local_new = (char*) malloc((rows_per_process * N + 2 * N) * sizeof(char));

    // Reading a file only in the master process
    if (rank == 0) {
        grid = (char*) malloc(N * N * sizeof(char));

        FILE* input = fopen(argv[2], "r");
        if (!input) { 
            fprintf(stderr, "Cannot open file %s\n", argv[2]);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        for (int i = 0; i < N; ++i) {
            fscanf(input, "%s", grid + i * N);
        }
        fclose(input);
    }
    // Send chunks playing field processes
    MPI_Scatter(grid, rows_per_process * N, MPI_CHAR, 
                grid_local + N, rows_per_process * N, MPI_CHAR, 0, MPI_COMM_WORLD);

    // 1 and 2 - request recieve, 3 and 4 request send
    MPI_Request reqs[4];
    MPI_Status stats[4];
    
    for (int iter = 0; iter < iterations; ++iter) {
        // Get the upper bound the neighbor (this is the bottom line of the shadow points)
        MPI_Irecv(grid_local + (rows_per_process + 1) * N, N, MPI_CHAR, 
                 (rank + 1 == nprocesses ? 0 : rank + 1) , 1, MPI_COMM_WORLD, &reqs[0]);
        
        // Get the lower bound the neighbor (this is the top line of the shadow points)
        MPI_Irecv(grid_local, N, MPI_CHAR, (rank - 1 == -1 ? nprocesses - 1 : rank - 1), 
                  2, MPI_COMM_WORLD, &reqs[1]);

                         
        // Send upper bound
        MPI_Isend(grid_local + N, N, MPI_CHAR, (rank - 1 == -1 ? nprocesses - 1 : rank - 1), 
                  1, MPI_COMM_WORLD, &reqs[2]);
                  
        // Send lower bound
        MPI_Isend(grid_local + rows_per_process * N, N, MPI_CHAR,
                  (rank + 1 == nprocesses ? 0 : rank + 1), 2, MPI_COMM_WORLD, &reqs[3]);                                    
               

        // Skip the shadow points and bound
        for (int i = 2; i < rows_per_process; ++i) {
            for (int j = 0; j < N; ++j) {
                int alive_count = 0;
                for (int di = -1; di <= 1; ++di) {
                    for (int dj = -1; dj <= 1; ++dj) {
                        if ((di != 0 || dj != 0) && grid_local[toindex(i + di, j + dj, rows_per_process + 2, N)] == ALIVE) { 
                            ++alive_count;
                        }
                    }
                }
                int current = i * N + j;
                if (alive_count == 3 || (alive_count == 2 && grid_local[current] == ALIVE)) {
                    grid_local_new[current] = ALIVE;
                } else {
                    grid_local_new[current] = DEAD;
                }
            }
        }
        MPI_Waitall(4, reqs, stats);

        // Only bound
        for (int l = 1; l < rows_per_process + 1; l = l + (rows_per_process - 1)) {
            for (int m = 0; m < N; ++m) {
                int alive_count = 0;
                for (int dl = -1; dl <= 1; ++dl) {
                    for (int dm = -1; dm <= 1; ++dm) {
                        if ((dl != 0 || dm != 0) && grid_local[toindex(l + dl, m + dm, rows_per_process + 2, N)] == ALIVE) { 
                            ++alive_count;
                        }
                    }
                } 
                int current = l * N + m;
                if (alive_count == 3 || (alive_count == 2 && grid_local[current] == ALIVE)) {
                    grid_local_new[current] = ALIVE;
                } else {
                    grid_local_new[current] = DEAD;
                }
            }
        }
        // swap        
        char* tmp = grid_local; grid_local = grid_local_new; grid_local_new = tmp;        
    }
    // Get all chunks from the all processes
    MPI_Gather(grid_local + N, rows_per_process * N, MPI_CHAR, 
               grid, rows_per_process * N, MPI_CHAR, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        FILE* output = fopen(argv[4], "w");
        printgrid(grid, grid_local_new, output, N);
        fclose(output);
        free(grid);
    }
    free(grid_local);
    free(grid_local_new);
    
    totaltime = MPI_Wtime() - totaltime;
    printf("Process %d: totaltime=%.6f\n", rank, totaltime);
    
    MPI_Finalize();
    return 0;
}