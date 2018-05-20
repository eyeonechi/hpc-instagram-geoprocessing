/*
 * File    : instaprocess_c.c
 * Title   : HPC Instagram Geoprocessing
 * Author  : Ivan Ken Weng Chee 736901
 * Created : 12/04/2018
 * Purpose : COMP90024 2018S1 Assignment 1
 */

#include <assert.h>
#include <ctype.h>
#include <mpi.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Stores post coordinates */
typedef struct {
  double x;
	double y;
} point_t;

/* Geographical grid boxes */
typedef struct {
  char name[3];
  char column;
  char row;
  double xmin;
  double xmax;
  double ymin;
  double ymax;
  int count;
} grid_t;

/* Row of grid boxes */
typedef struct {
  char name;
  int count;
} row_t;

/* Column of grid boxes */
typedef struct {
  char name;
  int count;
} column_t;

/* Checks if a row exists in rows */
bool inRows(char row, row_t *rows, int n) {
  int i;
  for (i = 0; i < n; i ++) {
    if (rows[i].name == row) {
      return true;
    }
  }
  return false;
}

/* Checks if a column exists in columns */
bool inColumns(char column, column_t *columns, int n) {
  int i;
  for (i = 0; i < n; i ++) {
    if (columns[i].name == column) {
      return true;
    }
  }
  return false;
}

/* Compares grid_t structs */
int compareGrids(const void *a, const void *b) {
  grid_t *gridA = (grid_t *)a;
  grid_t *gridB = (grid_t *)b;
  return (gridB->count - gridA->count);
}

/* Compares row_t structs */
int compareRows(const void *a, const void *b) {
  row_t *rowA = (row_t *)a;
  row_t *rowB = (row_t *)b;
  return (rowB->count - rowA->count);
}

/* Compares column_t structs */
int compareColumns(const void *a, const void *b) {
  column_t *columnA = (column_t *)a;
  column_t *columnB = (column_t *)b;
  return (columnB->count - columnA->count);
}

/* Main function */
int main(int argc, char** argv) {

  // Initialises MPI variables
  int size, rank;
  MPI_Init(NULL, NULL);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  // Start timer on master
  clock_t tick, tock;
  if (rank == 0) {
    tick = clock();
  }

  // Check for correct number of arguments
  if (argc != 3) {
    printf("usage: %s gridfile instagramfile\n", argv[0]);
    MPI_Finalize();
    exit(EXIT_FAILURE);
  }

  // Important variables
  grid_t *grids;
  grid_t *gathergrids;
  row_t *rows;
  column_t *columns;
  point_t *localpoints;
  point_t *points;
  size_t len = 0;
  ssize_t read;

  int i;
  int j;
  int lines = 0;
  int ch = 0;
  int locallen;
  int remaining;
  int numpoints = 0;
  int numgrids = 0;
  int numrows = 0;
  int numcolumns = 0;
  int gridslen = 20;
  int found = 0;
  int pointslen = 1;
  int chunksize;
  double x, y;
  char *pch;
  char *pch2;
  char *line = NULL;
  char *gridfilePath = argv[1];
  char *instafilePath = argv[2];
  bool features = false;
  bool foundproperties = false;

  // Opens files
  FILE *gridfile = fopen(gridfilePath, "r");
  assert(gridfile != NULL);
  FILE *instafile = fopen(instafilePath, "r");
  assert(instafile != NULL);

  // Initialise array of grid_t, row_t, and column_t
  grids = (grid_t *)malloc(gridslen * sizeof(grid_t));
  assert(grids != NULL);
  rows = (row_t *)malloc(gridslen * sizeof(row_t));
  assert(rows != NULL);
  columns = (column_t *)malloc(gridslen * sizeof(column_t));
  assert(columns != NULL);
  for (i = 0; i < numgrids; i ++) {
    rows[i].name = '\0';
    columns[i].name = '\0';
    rows[i].count = 0;
    columns[i].count = 0;
  }

  // Convert gridfile into an array of grid_t structs
  while ((read = getline(&line, &len, gridfile)) != -1) {
    if (strcmp(line, "\"features\": [\n") == 0) {
      features = true;
      continue;
    } else if (strcmp(line, "]\n") == 0) {
      break;
    }

    // A line containing grid information is found
    if (features) {
      // Exploits the structure of the line to search for edge coordinates
      pch = strtok(line, ":");
      while (pch != NULL) {
        if (strcmp(pch, " { \"id\"") == 0) {
          grids[numgrids].count = 0;

          // Grid name
          pch = strtok(NULL, ":");
          strncpy(grids[numgrids].name, (pch + 2), 2);
          grids[numgrids].name[2] = '\0';

          // Checks whether row has been recorded
          grids[numgrids].row = grids[numgrids].name[0];
          if (!inRows(grids[numgrids].name[0], rows, numgrids)) {
            rows[numrows].name = grids[numgrids].name[0];
            numrows ++;
          }

          // Checks whether column has been recorded
          grids[numgrids].column = grids[numgrids].name[1];
          if (!inColumns(grids[numgrids].name[1], columns, numgrids)) {
            columns[numcolumns].name = grids[numgrids].name[1];
            numcolumns ++;
          }

          // Grid xmin
          pch = strtok(NULL, ":");
          grids[numgrids].xmin = strtod((pch + 1), NULL);

          // Grid xmax
          pch = strtok(NULL, ":");
          grids[numgrids].xmax = strtod((pch + 1), NULL);

          // Grid ymin
          pch = strtok(NULL, ":");
          grids[numgrids].ymin = strtod((pch + 1), NULL);

          // Grid ymax
          pch = strtok(NULL, ":");
          grids[numgrids].ymax = strtod((pch + 1), NULL);
          break;
        }
        pch = strtok(NULL, ":");
      }
      numgrids ++;

      // Resizes grids array
      if (numgrids >= gridslen) {
        gridslen *= 2;
        grids = realloc(grids, gridslen * sizeof(grid_t));
        assert(grids != NULL);
      }
    }
  }

  // Master
  if (rank == 0) {

    // Multi core
    if (size > 1) {

      // Initialises array of point_t structs
      points = (point_t *)malloc(pointslen * sizeof(point_t));
      assert(points != NULL);

      // Process the Instagramfile, storing points as they are found
      for (i = 0; (read = getline(&line, &len, instafile)) != -1; i ++) {
        lines ++;

        // Exploits the structure of the line to search for (y,x) coordinates
        pch = strtok(line, ":");
        while (pch != NULL) {
          // A point is found
          if (found == 1) {
            pch2 = strtok(pch, ",");
            pch2 ++;
            y = strtod(pch2, NULL);
            pch2 = strtok(NULL, ",");
            x = strtod(pch2, NULL);
            points[numpoints].x = x;
            points[numpoints].y = y;
            numpoints ++;

            // Resizes points array
            if (numpoints >= pointslen) {
              pointslen *= 2;
              points = realloc(points, pointslen * sizeof(point_t));
              assert(points != NULL);
            }
          }
          if (strcmp(pch, "\"Point\",\"coordinates\"") == 0) {
            found = 1;
          } else {
            found = 0;
          }
          pch = strtok(NULL, ":");
        }
      }

      chunksize = numpoints / size;
      locallen = chunksize;
      remaining = numpoints;

      // Sends numpoints and chunksize to workers
      for (i = 1; i < size; i ++) {
        MPI_Send(&chunksize, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
        MPI_Send(&numpoints, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
      }
    }

    // Single core
    else {
      points = (point_t *)malloc(pointslen * sizeof(point_t));
      assert(points != NULL);

      // Same process as multicore excluding MPI calls
      for (i = 0; (read = getline(&line, &len, instafile)) != -1; i ++) {
        lines ++;
        pch = strtok(line, ":");
        while (pch != NULL) {
          if (found == 1) {
            pch2 = strtok(pch, ",");
            pch2 ++;
            y = strtod(pch2, NULL);
            pch2 = strtok(NULL, ",");
            x = strtod(pch2, NULL);
            points[numpoints].x = x;
            points[numpoints].y = y;
            numpoints ++;
            if (numpoints >= pointslen) {
              pointslen *= 2;
              points = realloc(points, pointslen * sizeof(point_t));
              assert(points != NULL);
            }
          }
          if (strcmp(pch, "\"Point\",\"coordinates\"") == 0) {
            found = 1;
          } else {
            found = 0;
          }
          pch = strtok(NULL, ":");
        }
      }
    }
  }

  // Worker
  else {
    // Receives numpoints and chunksize from Master
    MPI_Recv(&chunksize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&numpoints, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    locallen = chunksize;
    remaining = numpoints;
  }

  // Prepares to scatter points array to all processes
  if (size > 1) {
    int sendcounts[size];
    int senddispls[size];
    for (i = 0; i < size; i ++) {
      senddispls[i] = i * 2 * locallen;
      if (i == (size - 1)) {
        sendcounts[i] = 2 * remaining;
      } else {
        sendcounts[i] = 2 * locallen;
        remaining -= locallen;
      }
    }

    // Deals with uneven array splitting
    if (rank == (size - 1)) {
      locallen = remaining;
    }
    localpoints = (point_t *)malloc(locallen * sizeof(point_t));
    assert(localpoints != NULL);

    // Scatters the points array
    MPI_Scatterv(points, sendcounts, senddispls, MPI_DOUBLE, localpoints, 2 * locallen, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // Counts the number of points in respective grid boxes
    for (i = 0; i < locallen; i ++) {
      for (j = 0; j < numgrids; j ++) {
        if (localpoints[i].x >= grids[j].xmin
          && localpoints[i].x <= grids[j].xmax
          && localpoints[i].y >= grids[j].ymin
          && localpoints[i].y <= grids[j].ymax) {
          grids[j].count ++;
        }
      }
    }
  }

  // Master continues alone
  else {
    for (i = 0; i < numpoints; i ++) {
      for (j = 0; j < numgrids; j ++) {
        if (points[i].x >= grids[j].xmin
          && points[i].x <= grids[j].xmax
          && points[i].y >= grids[j].ymin
          && points[i].y <= grids[j].ymax) {
          grids[j].count ++;
        }
      }
    }
  }

  // Ensures processes are synchronised before combining results
  MPI_Barrier(MPI_COMM_WORLD);

  // Prepares to combine results
  if (size > 1) {
    int counts[numgrids];
    int totalcounts[numgrids];
    // Copies grid post counts into an integer array
    for (i = 0; i < numgrids; i ++) {
      counts[i] = grids[i].count;
    }

    // Reduces the post counts array across all processes (Order of grid names is preserved)
    for (i = 0; i < numgrids; i ++) {
      MPI_Reduce((counts + i), (totalcounts + i), 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    }

    // Master calculates row and column post counts from the reduced array
    if (rank == 0) {
      for (i = 0; i < numgrids; i ++) {
        for (j = 0; j < numrows; j ++) {
          if (rows[j].name == grids[i].row) {
            rows[j].count += totalcounts[i];
            break;
          }
        }
        for (j = 0; j < numcolumns; j ++) {
          if (columns[j].name == grids[i].column) {
            columns[j].count += totalcounts[i];
          }
        }
        grids[i].count = totalcounts[i];
      }
    }
  } else {
    // Calculate row and column post counts
    for (i = 0; i < numgrids; i ++) {
      for (j = 0; j < numrows; j ++) {
        if (rows[j].name == grids[i].row) {
          rows[j].count += grids[i].count;
          break;
        }
      }
      for (j = 0; j < numcolumns; j ++) {
        if (columns[j].name == grids[i].column) {
          columns[j].count += grids[i].count;
        }
      }
    }
  }

  // Master finishes
  if (rank == 0) {

    // Sorts the post counts in decreasing order
    qsort(grids, numgrids, sizeof(grid_t), compareGrids);
    qsort(rows, numrows, sizeof(row_t), compareRows);
    qsort(columns, numcolumns, sizeof(column_t), compareColumns);

    // Prints the final output
    printf("// Rank by Unit\n");
    for (i = 0; i < numgrids; i ++) {
      printf("%s: %d posts\n", grids[i].name, grids[i].count);
    }
    printf("// Rank by Row\n");
    for (i = 0; i < numrows; i ++) {
      printf("%c-Row: %d posts\n", rows[i].name, rows[i].count);
    }
    printf("// Rank by Column\n");
    for (i = 0; i < numcolumns; i ++) {
      printf("Column %c: %d posts\n", columns[i].name, columns[i].count);
    }

    // Prints the time taken
    tock = clock();
    printf("Time: %lfs\n", ((float) tock - (float) tick) / CLOCKS_PER_SEC);
  }

  // Cleanup
  fclose(gridfile);
  fclose(instafile);

  MPI_Finalize();
  exit(EXIT_SUCCESS);
}
