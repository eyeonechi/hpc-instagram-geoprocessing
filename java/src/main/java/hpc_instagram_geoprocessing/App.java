package hpc_instagram_geoprocessing;

import java.io.*;
import java.util.*;
import mpi.*;

/**
 * Hello world!
 *
 */
public class App {
  public static void main(String[] args) throws Exception {
    MPI.Init(args);
    int rank = MPI.COMM_WORLD.Rank();
    int size = MPI.COMM_WORLD.Size();
    int master = 0;
    int tag = 100;

    // Master
    if (rank == master) {
      String filename = "../data/melbGrid.json";
      ArrayList<Grid> grids = new ArrayList<Grid>();
      Boolean features = false;

      try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
        String line;
        while ((line = br.readLine()) != null) {
          if (line.equals("\"features\": [")) {
            features = true;
            continue;
          } else if (line.equals("]")) {
            features = false;
          }
          if (features) {
            String[] words = line.split(" ");
            Grid grid = new Grid();
            for (int i = 0; i < words.length; i ++) {
              if (words[i].equals("\"id\":")) {
                grid.name = words[i + 1].replaceAll("\"", "").replaceAll(",", "");
              } else if (words[i].equals("\"xmin\":")) {
                grid.xmin = Float.parseFloat(words[i + 1].replaceAll(",", ""));
              } else if (words[i].equals("\"xmax\":")) {
                grid.xmax = Float.parseFloat(words[i + 1].replaceAll(",", ""));
              } else if (words[i].equals("\"ymin\":")) {
                grid.ymin = Float.parseFloat(words[i + 1].replaceAll(",", ""));
              } else if (words[i].equals("\"ymax\":")) {
                grid.ymax = Float.parseFloat(words[i + 1].replaceAll(",", ""));
              }
            }
            grids.add(grid);
          }
        }
        // Send grid coordinates to workers
        Object[] sendObjectArray = new Object[1];
        sendObjectArray[0] = grids.toArray();
        for (int i = 1; i < size; i ++) {
          MPI.COMM_WORLD.Send(sendObjectArray, 0, 1, MPI.OBJECT, i, tag);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

      int worker = 1;
      String filename2 = "../data/tinyInstagram.json";
      try (BufferedReader br = new BufferedReader(new FileReader(filename2))) {
        String line;
        String header = br.readLine(); //First row
        while ((line = br.readLine()) != null) {
          Object[] sendObjectArray = new Object[1];
          sendObjectArray[0] = (Object) line;
          // Send lines to workers alternatingly
          for (int i = 1; i < size; i ++) {
            MPI.COMM_WORLD.Send(sendObjectArray, 0, 1, MPI.OBJECT, i, tag);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    // Worker
    else {
      // Receive coordinates from master
      Object[][] recvGridsArray = new Object[1][];
      ArrayList<Grid> grids = new ArrayList<Grid>();
      MPI.COMM_WORLD.Recv(recvGridsArray, 0, 1, MPI.OBJECT, master, tag);
      for (int i = 0; i < recvGridsArray[0].length; i ++) {
        grids.add((Grid) recvGridsArray[0][i]);
      }

      for (Grid grid: grids) {
        System.out.println(grid);
      }

      // Receive lines from master
      Object[] recvObjectArray = new Object[1];
      String line = null;
      while (true) {
        MPI.COMM_WORLD.Recv(recvObjectArray, 0, 1, MPI.OBJECT, master, tag);
        line = (String) recvObjectArray[0];
        if (line.equals("]}")) {
          break;
        } else {
          String[] words = line.split(":");
          for (int i = 0; i < words.length; i ++) {
            if (words[i].equals("\"Point\",\"coordinates\"")) {
              String[] latlon = words[i + 1].replaceAll("]}}},", "").replace("[", "").split(",");
              Float x = Float.parseFloat(latlon[1]);
              Float y = Float.parseFloat(latlon[0]);
              System.out.println(x + ", " + y);
            }
          }
        }
      }
    }

    MPI.Finalize();
  }
}
