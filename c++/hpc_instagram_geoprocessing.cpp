#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <mpi.h>
using namespace std;

int main(int argc, char **argv) {

  int size, rank;
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  /* Master */
  if (rank == 0) {
    clock_t t1, t2;
    t1 = clock();

    fstream coords, insta;
    string line;
    bool features = false;
    bool post = false;
    int worker = 1;
    //const std::string Classname::FEATURES = "\"features\": [";

    coords.open("../data/melbGrid.json", ios::in);
    if (coords.is_open()) {
      while (getline(coords, line)) {
        if (line.find("\"features\": [") != string::npos) {
          features = true;
          continue;
        } else if (line == "]\r") {
          features = false;
        }
        if (features) {
          cout << line << endl;
        }
      }
      coords.close();
    }

    insta.open("../data/tinyInstagram.json", ios::in);
    if (insta.is_open()) {
      while (getline(insta, line)) {
        if (line.find("\"rows\":[") != string::npos) {
          post = true;
          continue;
        } else if (line == "]}") {
          post = false;
          for (int i = 1; i < size; i ++) {
            MPI::COMM_WORLD.Send(line.c_str(), line.length(), MPI::CHAR, i, 1);
          }
        }
        if (post) {
          MPI::COMM_WORLD.Send(line.c_str(), line.length(), MPI::CHAR, worker, 1);
          // Change worker
          if (worker < (size - 1)) {
            worker ++;
          } else {
            worker = 1;
          }
        }
      }
      insta.close();
    }

    t2 = clock();
    float diff = ((float) t2 - (float) t1) / CLOCKS_PER_SEC;
    cout << "Time: " << diff << "s" << endl;
  }

  /* Worker */
  else {
    int master = 0;
    while (true) {
      MPI::Status status;
      MPI::COMM_WORLD.Probe(master, 1, status);
      int length = status.Get_count(MPI::CHAR);
      char *buffer = new char[length];
      MPI::COMM_WORLD.Recv(buffer, length, MPI_CHAR, master, 1, status);
      string line(buffer, length);
      delete [] buffer;
      if (line == "]}") {
        break;
      }
      //cout << rank << ": " << line << endl;
    }
  }

  MPI_Finalize();
  return(0);
}
