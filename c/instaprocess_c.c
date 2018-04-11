#include <assert.h>
#include <ctype.h>
#include <mpi.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
  double x;
	double y;
} point_t;

typedef struct {
  char name;
  int count;
} row_t;

typedef struct {
  char name;
  int count;
} column_t;

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

bool inRows(char row, row_t *rows, int n) {
  int i;
  for (i = 0; i < n; i ++) {
    if (rows[i].name == row) {
      return true;
    }
  }
  return false;
}

bool inColumns(char column, column_t *columns, int n) {
  int i;
  for (i = 0; i < n; i ++) {
    if (columns[i].name == column) {
      return true;
    }
  }
  return false;
}

int compareGrids(const void *a, const void *b) {
  grid_t *gridA = (grid_t *)a;
  grid_t *gridB = (grid_t *)b;
  return (gridB->count - gridA->count);
}

int compareRows(const void *a, const void *b) {
  row_t *rowA = (row_t *)a;
  row_t *rowB = (row_t *)b;
  return (rowB->count - rowA->count);
}

int compareColumns(const void *a, const void *b) {
  column_t *columnA = (column_t *)a;
  column_t *columnB = (column_t *)b;
  return (columnB->count - columnA->count);
}

int main(int argc, char** argv) {
  MPI_Init(NULL, NULL);
  int size, rank, processor;
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Get_processor_name(processor_name, &processor);
  clock_t tick, tock;

  if (argc != 3) {
    exit(EXIT_FAILURE);
  }
  char *gridfilePath = argv[1];
  char *instafilePath = argv[2];

  FILE *gridfile = fopen(gridfilePath, "r");
  assert(gridfile != NULL);
  FILE *instafile = fopen(instafilePath, "r");
  assert(instafile != NULL);

  grid_t *grids;
  row_t *rows;
  column_t *columns;
  char *pch;
  char *pch2;

  char *line = NULL;
  size_t len = 0;
  ssize_t read;
  int i;
  int j;
  int lines = 0;
  int ch = 0;

  point_t *localpoints;
  int locallen;
  point_t *points;
  int remaining;
  int numpoints = 0;
  bool features = false;
  bool foundproperties = false;

  grid_t *gathergrids;

  // Initialise grids
  int numgrids = 0;
  int gridslen = 20;
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
  int numrows = 0;
  int numcolumns = 0;

  // Process gridfile
  while ((read = getline(&line, &len, gridfile)) != -1) {
    if (strcmp(line, "\"features\": [\n") == 0) {
      features = true;
      continue;
    } else if (strcmp(line, "]\n") == 0) {
      break;
    }
    if (features) {
      pch = strtok(line, ":");
      while (pch != NULL) {
        if (strcmp(pch, " { \"id\"") == 0) {
          grids[numgrids].count = 0;
          // name
          pch = strtok(NULL, ":");
          strncpy(grids[numgrids].name, (pch + 2), 2);
          grids[numgrids].name[2] = '\0';
          // row
          grids[numgrids].row = grids[numgrids].name[0];
          if (!inRows(grids[numgrids].name[0], rows, numgrids)) {
            rows[numrows].name = grids[numgrids].name[0];
            numrows ++;
          }
          // column
          grids[numgrids].column = grids[numgrids].name[1];
          if (!inColumns(grids[numgrids].name[1], columns, numgrids)) {
            columns[numcolumns].name = grids[numgrids].name[1];
            numcolumns ++;
          }
          // xmin
          pch = strtok(NULL, ":");
          grids[numgrids].xmin = strtod((pch + 1), NULL);
          // xmax
          pch = strtok(NULL, ":");
          grids[numgrids].xmax = strtod((pch + 1), NULL);
          // ymin
          pch = strtok(NULL, ":");
          grids[numgrids].ymin = strtod((pch + 1), NULL);
          // ymax
          pch = strtok(NULL, ":");
          grids[numgrids].ymax = strtod((pch + 1), NULL);
          break;
        }
        pch = strtok(NULL, ":");
      }
      numgrids ++;
      // Resize grids array
      if (numgrids >= gridslen) {
        gridslen *= 2;
        grids = realloc(grids, gridslen * sizeof(grid_t));
        assert(grids != NULL);
      }
    }
  }
/*
  for (i = 0; i < numrows; i ++) {
    printf("%c, ", rows[i].name);
  }
  printf("\n");
  for (i = 0; i < numcolumns; i ++) {
    printf("%c, ", columns[i].name);
  }
  printf("\n");

  for (i = 0; i < numgrids; i ++) {
    printf("name:%s, ", grids[i].name); //name
    printf("row:%c, ", grids[i].row); //row
    printf("col:%c, ", grids[i].column); //col
    printf("xmin:%lf, ", grids[i].xmin); //xmin
    printf("xmax:%lf, ", grids[i].xmax); //xmax
    printf("ymin:%lf, ", grids[i].ymin); //ymin
    printf("ymax:%lf, ", grids[i].ymax); //ymax
    printf("\n");
  }
*/

  // Master
  if (rank == 0) {
    tick = clock();

    char *coordinates = "\"Point\",\"coordinates\"";
    // Multi core
    if (size > 1) {
      //char *pch;
      //char *pch2;
      int found = 0;
      double x, y;

      int pointslen = 1;
      points = (point_t *)malloc(pointslen * sizeof(point_t));
      assert(points != NULL);

      // Process first chunk
      for (i = 0; (read = getline(&line, &len, instafile)) != -1; i ++) {
        //printf("%d: %zu: %s", i, read, line);
        lines ++;
        pch = strtok(line, ":");
        while (pch != NULL) {
          if (found == 1) {
            //printf("%s\n", pch);
            //MPI_Send(pch, strlen(pch), MPI_CHAR, 1, 0, MPI_COMM_WORLD);

            pch2 = strtok(pch, ",");
            pch2 ++;
            y = strtod(pch2, NULL);
            pch2 = strtok(NULL, ",");
            x = strtod(pch2, NULL);

            points[numpoints].x = x;
            points[numpoints].y = y;
            numpoints ++;

            // Resize points array
            if (numpoints >= pointslen) {
              pointslen *= 2;
              points = realloc(points, pointslen * sizeof(point_t));
              assert(points != NULL);
            }

          }
          if (strcmp(pch, coordinates) == 0) {
            found = 1;
          } else {
            found = 0;
          }
          pch = strtok(NULL, ":");
        }
      }

      int chunksize = numpoints / size;
      locallen = chunksize;
      remaining = numpoints;
      // Send numpoints and chunksize to workers
      for (i = 1; i < size; i ++) {
        MPI_Send(&chunksize, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
        MPI_Send(&numpoints, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
      }
/*
      printf("POINTS");
      for (i = 0; i < numpoints; i ++) {
        printf("[%4.2lf,%4.2lf],", points[i].x, points[i].y);
      }
*/
    }
    // Single core
    else {
      int found = 0;
      double x, y;
      int pointslen = 1;
      points = (point_t *)malloc(pointslen * sizeof(point_t));
      assert(points != NULL);
      // Process first chunk
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
            // Resize points array
            if (numpoints >= pointslen) {
              pointslen *= 2;
              points = realloc(points, pointslen * sizeof(point_t));
              assert(points != NULL);
            }
          }
          if (strcmp(pch, coordinates) == 0) {
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
    /*
        int n;
        MPI_Status status;
        MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_BYTE, &n);
    */

    int chunksize;
    MPI_Recv(&chunksize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&numpoints, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    locallen = chunksize;
    remaining = numpoints;
  }

  MPI_Barrier(MPI_COMM_WORLD);

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
    // Set last rank to the special size
    if (rank == (size - 1)) {
      locallen = remaining;
    }

    localpoints = (point_t *)malloc(locallen * sizeof(point_t));
    assert(localpoints != NULL);

    MPI_Scatterv(points, sendcounts, senddispls, MPI_DOUBLE, localpoints, 2 * locallen, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // Check coordinates
    for (i = 0; i < locallen; i ++) {
      //printf("[%4.2lf,%4.2lf],", localpoints[i].x, localpoints[i].y);
      for (j = 0; j < numgrids; j ++) {
        if (localpoints[i].x >= grids[j].xmin
          && localpoints[i].x <= grids[j].xmax
          && localpoints[i].y >= grids[j].ymin
          && localpoints[i].y <= grids[j].ymax) {
          grids[j].count ++;
        }
      }
    }
  } else {
    // Check coordinates
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


/*
  for (i = 0; i < numgrids; i ++) {
    printf("%s: %d, ", grids[i].name, grids[i].count);
  }
  printf("\n");
  */

  MPI_Barrier(MPI_COMM_WORLD);
/*
  gathergrids = (grid_t *)malloc(size * numgrids * sizeof(grid_t));
  assert(gathergrids != NULL);
  int recvdispls[size];
  int recvcounts[size];

  for (i = 0; i < size; i ++) {
    recvdispls[i] = i * sizeof(grid_t) * numgrids;
    recvcounts[i] = sizeof(grid_t) * numgrids;
  }

  MPI_Gatherv(grids, numgrids * sizeof(grid_t), MPI_BYTE, gathergrids, recvcounts, recvdispls, MPI_BYTE, 0, MPI_COMM_WORLD);
  */

  if (size > 1) {
    int counts[numgrids];
    int totalcounts[numgrids];
    for (i = 0; i < numgrids; i ++) {
      counts[i] = grids[i].count;
    }
    for (i = 0; i < numgrids; i ++) {
      MPI_Reduce((counts + i), (totalcounts + i), 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    }

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

      qsort(grids, numgrids, sizeof(grid_t), compareGrids);
      qsort(rows, numrows, sizeof(row_t), compareRows);
      qsort(columns, numcolumns, sizeof(column_t), compareColumns);

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
    }
  } else {
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

    qsort(grids, numgrids, sizeof(grid_t), compareGrids);
    qsort(rows, numrows, sizeof(row_t), compareRows);
    qsort(columns, numcolumns, sizeof(column_t), compareColumns);

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
  }

  MPI_Barrier(MPI_COMM_WORLD);

  if (rank == 0) {
    tock = clock();
    printf("Time: %lfs\n", ((float) tock - (float) tick) / CLOCKS_PER_SEC);
  }

  fclose(gridfile);
  fclose(instafile);
  if (line) {
    free(line);
  }

  MPI_Finalize();
  exit(EXIT_SUCCESS);
}
