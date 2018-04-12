/*
 * File    : instaprocess_cpp.cpp
 * Title   : HPC Instagram Geoprocessing
 * Author  : Ivan Ken Weng Chee 736901
 * Created : 13/04/2018
 * Purpose : COMP90024 2018S1 Assignment 1
 */

#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <vector>

#include <mpi.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

/* Stores post coordinates */
class Point {
  public:
    double x;
    double y;
};

/* Geographical grid boxes */
class Grid {
  public:
    string name;
    char column;
    char row;
    double xmin;
    double xmax;
    double ymin;
    double ymax;
    int count;
};

/* Row of grid boxes */
class Row {
  public:
    char name;
    int count;
};

/* Column of grid boxes */
class Column {
  public:
    char name;
    int count;
};

/* Checks if a row exists in rows */
bool inRows(char row, vector<Row> rows) {
  for (int i = 0; i < rows.size(); i ++) {
    if (rows[i].name == row) {
      return true;
    }
  }
  return false;
}

/* Checks if a column exists in columns */
bool inColumns(char column, vector<Column> columns) {
  for (int i = 0; i < columns.size(); i ++) {
    if (columns[i].name == column) {
      return true;
    }
  }
  return false;
}

/* Compares Grid class objects */
bool gridCmp(Grid const &a, Grid const &b) {
  return(a.count > b.count);
}

/* Compares Row class objects */
bool rowCmp(Row const &a, Row const &b) {
  return(a.count > b.count);
}

/* Compares Column class objects */
bool columnCmp(Column const &a, Column const &b) {
  return(a.count > b.count);
}

/* Main function */
int main(int argc, char **argv) {

  // Initialise MPI variables
  int size, rank;
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  // Start timer on master
  clock_t tick, tock;
  if (rank == 0) {
    tick = clock();
  }

  // Check for correct number of arguments
  if (argc != 3) {
    cerr << "usage: " << argv[0] << " gridfile instagramfile" << endl;
    MPI_Finalize();
    return 1;
  }

  // Important variables
  vector<Point> points;
  vector<Grid> grids;
  vector<Row> rows;
  vector<Column> columns;

  Point *point;
  Grid *grid;
  Row *row;
  Column *column;

  char *pch;
  char *pch2;
  bool features = false;
  bool found = false;
  double x, y;
  string line;

  // Gridfile
  fstream gridfile;
  gridfile.open(argv[1], ios::in);
  if (not gridfile) {
    cerr << "file: " << argv[1] << " failed to open" << endl;
    MPI_Finalize();
    return 1;
  }

  // Convert gridfile into a vector of Grids
  if (gridfile.is_open()) {
    while (getline(gridfile, line)) {
      if (line == "\"features\": [") {
        features = true;
        continue;
      } else if (line == "]") {
        features = false;
        break;
      }
      // A line containing grid information is found
      if (features) {
        char *cstr = new char[line.length() + 1];
        strcpy(cstr, line.c_str());

        // Exploits the structure of the line to search for edge coordinates
        pch = strtok(cstr, ":");
        while (pch != NULL) {
          if (strcmp(pch, " { \"id\"") == 0) {
            grid = new Grid;
            grid->count = 0;

            // Grid name
            pch = strtok(NULL, ":");
            char cname[3];
            strncpy(cname, (pch + 2), 2);
            cname[2] = '\0';
            grid->name = cname;

            // Checks whether row has been recorded
            grid->row = grid->name[0];
            if (!inRows(grid->name[0], rows)) {
              row = new Row;
              row->name = grid->name[0];
              rows.push_back(*row);
            }

            // Checks whether column has been recorded
            grid->column = grid->name[1];
            if (!inColumns(grid->name[1], columns)) {
              column = new Column;
              column->name = grid->name[1];
              columns.push_back(*column);
            }

            // Grid xmin
            pch = strtok(NULL, ":");
            grid->xmin = strtod((pch + 1), NULL);

            // Grid xmax
            pch = strtok(NULL, ":");
            grid->xmax = strtod((pch + 1), NULL);

            // Grid ymin
            pch = strtok(NULL, ":");
            grid->ymin = strtod((pch + 1), NULL);

            // Grid ymax
            pch = strtok(NULL, ":");
            grid->ymax = strtod((pch + 1), NULL);
            grids.push_back(*grid);
            break;
          }
          pch = strtok(NULL, ":");
        }
      }
    }
    gridfile.close();
  }

  // Instagram file
  ifstream file(argv[2], ifstream::binary);
  if (not file) {
    cerr << "file: " << argv[2] << " failed to open" << endl;
    MPI_Finalize();
    return 1;
  }

  // Get file size without seeking to the end
  struct stat filestatus;
  stat(argv[2], &filestatus);

  // Calculates the chunk size and number of chunks
  size_t total_size = filestatus.st_size;
  size_t chunk_size = total_size / size;
  size_t total_chunks = size;
  size_t last_chunk_size = total_size % chunk_size;

  // If the file splits unevenly, add an unfilled final chunk to the last chunk
  if (last_chunk_size != 0) {
    last_chunk_size += chunk_size;
  }
  // If file splits evenly, the last chunk is full
  else {
    last_chunk_size = chunk_size;
  }
  size_t chunk = rank;
  size_t this_chunk_size = (chunk == total_chunks - 1) ? last_chunk_size : chunk_size;

  // Seeks to the start position of this chunk in the file
  size_t start_of_chunk = chunk * chunk_size;
  file.seekg(start_of_chunk, ios::beg);

  // Process the chunk, storing points as they are found
  for (size_t i = 0; i < chunk_size && getline(file, line); i += line.length()) {
    char *cstr = new char[line.length() + 1];
    strcpy(cstr, line.c_str());

    // Exploits the structure of the line to search for (y,x) coordinates
    pch = strtok(cstr, ":");
    while (pch != NULL) {
      // A point is found
      if (found) {
        point = new Point;
        pch2 = strtok(pch, ",");
        pch2 ++;
        point->y = strtod(pch2, NULL);
        pch2 = strtok(NULL, ",");
        point->x = strtod(pch2, NULL);
        points.push_back(*point);
        found = false;
      }
      if (strcmp(pch, "\"Point\",\"coordinates\"") == 0) {
        found = true;
      } else {
        found = false;
      }
      pch = strtok(NULL, ":");
    }
    delete [] cstr;
  }

  // Counts the number of points in respective grid boxes
  for (int i = 0; i < points.size(); i ++) {
    for (int j = 0; j < grids.size(); j ++) {
      if (
        points[i].x >= grids[j].xmin
        && points[i].x <= grids[j].xmax
        && points[i].y >= grids[j].ymin
        && points[i].y <= grids[j].ymax
      ) {
        grids[j].count ++;
      }
    }
  }

  int counts[grids.size()];
  int totalcounts[grids.size()];
  // Copies grid post counts into an integer array
  for (int i = 0; i < grids.size(); i ++) {
    counts[i] = grids[i].count;
  }

  // Reduces the post counts array across all processes (Order of grid names is preserved)
  for (int i = 0; i < grids.size(); i ++) {
    MPI_Reduce((counts + i), (totalcounts + i), 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  }

  // Master does the remaining work
  if (rank == 0) {
    // Calculate row and column post counts from the reduced array
    for (int i = 0; i < grids.size(); i ++) {
        for (int j = 0; j < rows.size(); j ++) {
          if (rows[j].name == grids[i].row) {
            rows[j].count += totalcounts[i];
            break;
          }
        }
        for (int j = 0; j < columns.size(); j ++) {
          if (columns[j].name == grids[i].column) {
            columns[j].count += totalcounts[i];
          }
        }
        grids[i].count = totalcounts[i];
      }

    // Sorts the post counts in decreasing order
    sort(grids.begin(), grids.end(), gridCmp);
    sort(rows.begin(), rows.end(), rowCmp);
    sort(columns.begin(), columns.end(), columnCmp);

    // Prints the final output
    cout <<"// Rank by Unit" << endl;
    for (int i = 0; i < grids.size(); i ++) {
      cout << grids[i].name <<": " << grids[i].count << " posts" << endl;
    }
    cout << "// Rank by Row" << endl;
    for (int i = 0; i < rows.size(); i ++) {
      cout << rows[i].name << "-Row: " << rows[i].count << " posts" << endl;
    }
    cout << "// Rank by Column" << endl;
    for (int i = 0; i < columns.size(); i ++) {
      cout << "Column " << columns[i].name << ": " << columns[i].count << " posts" << endl;
    }

    // Prints the time taken
    tock = clock();
    float diff = ((float) tock - (float) tick) / CLOCKS_PER_SEC;
    cout << "Time: " << diff << "s" << endl;
  }

  MPI_Finalize();
  return(0);
}
