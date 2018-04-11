import java.io.*;
import java.util.*;
import mpi.*;

public class HPCInstagramGeoprocessing {

  public static void main(String[] args) throws Exception {
    long startTime = 0;
    long endTime = 0;
    MPI.Init(args);
    int rank = MPI.COMM_WORLD.Rank();
    int size = MPI.COMM_WORLD.Size();
    int master = 0;
    int tag = 100;

    Object[] postsArray = new Object[1];
    Object[] rowsArray = new Object[1];
    Object[] columnsArray = new Object[1];

    HashMap<String, Integer> posts = new HashMap<String, Integer>();
    HashMap<String, Integer> rows = new HashMap<String, Integer>();
    HashMap<String, Integer> columns = new HashMap<String, Integer>();

    // Master
    if (rank == master) {
      if (args.length != 5) {
        System.exit(1);
      }
      String gridfile = args[args.length - 2];
      String instafile = args[args.length - 1];

      startTime = System.nanoTime();
      postsArray[0] = posts;
      rowsArray[0] = rows;
      columnsArray[0] = columns;

      ArrayList<Grid> grids = new ArrayList<Grid>();
      Boolean features = false;
      BufferedReader br = new BufferedReader(new FileReader(gridfile));
      try {
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
                grid.row = grid.name.substring(0, 1);
                grid.column = grid.name.substring(1, 2);
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
      } catch (IOException e) {
        e.printStackTrace();
      }

      // Multi core
      if (size > 1) {
        // Send grid coordinates to workers
        Object[] sendObjectArray = new Object[1];
        sendObjectArray[0] = grids.toArray();
        for (int i = 1; i < size; i ++) {
          MPI.COMM_WORLD.Send(sendObjectArray, 0, 1, MPI.OBJECT, i, tag);
        }

        int worker = 1;
        br = new BufferedReader(new FileReader(instafile));
        try {
          String line;
          String header = br.readLine(); //First row
          while ((line = br.readLine()) != null) {
            // Send terminating signal
            if (line.equals("]}")) {
              Object[] terminateObjectArray = new Object[1];
              terminateObjectArray[0] = (Object) "terminate";
              for (int i = 1; i < size; i ++) {
                MPI.COMM_WORLD.Send(terminateObjectArray, 0, 1, MPI.OBJECT, i, tag);
              }
              break;
            }
            sendObjectArray[0] = (Object) line;
            // Send lines to workers alternatingly
            MPI.COMM_WORLD.Send(sendObjectArray, 0, 1, MPI.OBJECT, worker, tag);
            // Change worker
            if (worker < (size - 1)) {
              worker += 1;
            } else {
              worker = 1;
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      // Single core
      else {
        for (Grid grid: grids) {
          posts.put(grid.name, 0);
          if (!rows.containsKey(grid.row)) {
            rows.put(grid.row, 0);
          }
          if (!columns.containsKey(grid.column)) {
            columns.put(grid.column, 0);
          }
        }
        br = new BufferedReader(new FileReader(instafile));
        try {
          String line;
          String header = br.readLine(); //First row
          while ((line = br.readLine()) != null) {
            // Send terminating signal
            if (line.equals("]}")) {
              break;
            }
            Integer temp = 0;
            String[] words = line.split(":");
            for (int i = 0; i < words.length; i ++) {
              if (words[i].equals("\"Point\",\"coordinates\"")) {
                String[] latlon = words[i + 1].replaceAll("]}}},", "").replace("[", "").split(",");
                try {
                  Float x = Float.parseFloat(latlon[1]);
                  Float y = Float.parseFloat(latlon[0]);
                  for (Grid grid: grids) {
                    if (grid.inGrid(x, y)) {
                      // Increment hash map
                      temp = posts.get(grid.name);
                      posts.remove(grid.name);
                      posts.put(grid.name, temp + 1);

                      temp = rows.get(grid.row);
                      rows.remove(grid.row);
                      rows.put(grid.row, temp + 1);

                      temp = columns.get(grid.column);
                      columns.remove(grid.column);
                      columns.put(grid.column, temp + 1);

                      break;
                    }
                  }
                } catch (NumberFormatException e) {
                  break;
                }
              }
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
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

      // Receive lines from master
      Object[] recvObjectArray = new Object[1];
      String line = null;

      for (Grid grid: grids) {
        posts.put(grid.name, 0);
        if (!rows.containsKey(grid.row)) {
          rows.put(grid.row, 0);
        }
        if (!columns.containsKey(grid.column)) {
          columns.put(grid.column, 0);
        }
      }

      while (true) {
        MPI.COMM_WORLD.Recv(recvObjectArray, 0, 1, MPI.OBJECT, master, tag);
        line = (String) recvObjectArray[0];
        if (line.equals("terminate")) {
          break;
        } else {
          Integer temp = 0;
          String[] words = line.split(":");
          for (int i = 0; i < words.length; i ++) {
            if (words[i].equals("\"Point\",\"coordinates\"")) {
              String[] latlon = words[i + 1].replaceAll("]}}},", "").replace("[", "").split(",");
              try {
                Float x = Float.parseFloat(latlon[1]);
                Float y = Float.parseFloat(latlon[0]);
                for (Grid grid: grids) {
                  if (grid.inGrid(x, y)) {
                    // Increment hash map
                    temp = posts.get(grid.name);
                    posts.remove(grid.name);
                    posts.put(grid.name, temp + 1);

                    temp = rows.get(grid.row);
                    rows.remove(grid.row);
                    rows.put(grid.row, temp + 1);

                    temp = columns.get(grid.column);
                    columns.remove(grid.column);
                    columns.put(grid.column, temp + 1);

                    break;
                  }
                }
              } catch (NumberFormatException e) {
                break;
              }
            }
          }
        }
      }
      postsArray[0] = (Object) posts;
      rowsArray[0] = (Object) rows;
      columnsArray[0] = (Object) columns;
    }

    MPI.COMM_WORLD.Barrier();
    if (size > 1) {
      HashMerge hashMerge = new HashMerge();
      Op op = new Op(hashMerge, true);
      MPI.COMM_WORLD.Reduce(postsArray, 0, postsArray, 0, 1, MPI.OBJECT, op, master);
      MPI.COMM_WORLD.Reduce(rowsArray, 0, rowsArray, 0, 1, MPI.OBJECT, op, master);
      MPI.COMM_WORLD.Reduce(columnsArray, 0, columnsArray, 0, 1, MPI.OBJECT, op, master);
    }

    List<Map.Entry<String, Integer>> sortedPosts = new ArrayList<>(posts.entrySet());
    Collections.sort(sortedPosts, new Comparator<Map.Entry<String, Integer>>() {
      public int compare(Map.Entry<String, Integer> x, Map.Entry<String, Integer> y) {
        return y.getValue().compareTo(x.getValue());
      }
    });
    List<Map.Entry<String, Integer>> sortedRows = new ArrayList<>(rows.entrySet());
    Collections.sort(sortedRows, new Comparator<Map.Entry<String, Integer>>() {
      public int compare(Map.Entry<String, Integer> x, Map.Entry<String, Integer> y) {
        return y.getValue().compareTo(x.getValue());
      }
    });
    List<Map.Entry<String, Integer>> sortedColumns = new ArrayList<>(columns.entrySet());
    Collections.sort(sortedColumns, new Comparator<Map.Entry<String, Integer>>() {
      public int compare(Map.Entry<String, Integer> x, Map.Entry<String, Integer> y) {
        return y.getValue().compareTo(x.getValue());
      }
    });

    if (rank == master) {
      System.out.println("// Rank by unit");
      for (Map.Entry<String, Integer> entry: sortedPosts) {
        System.out.println(entry.getKey() + ": " + entry.getValue() + " posts");
      }
      System.out.println("// Rank by row");
      for (Map.Entry<String, Integer> entry: sortedRows) {
        System.out.println(entry.getKey() + "-Row: " + entry.getValue() + " posts");
      }
      System.out.println("// Rank by column");
      for (Map.Entry<String, Integer> entry: sortedColumns) {
        System.out.println("Column " + entry.getKey() + ": " + entry.getValue() + " posts");
      }
      endTime = System.nanoTime();
      System.out.println("Time: " + ((endTime - startTime) / 1000000000) + "s");
    }

    MPI.Finalize();
  }

}
