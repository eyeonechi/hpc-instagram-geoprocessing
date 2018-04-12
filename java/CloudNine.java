import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.io.FileNotFoundException;

import mpi.*;

public class CloudNine {

	private static int contTag = 99;
	private static int exitTag = 11;
	private static int notFound = 99999;
	private static String melbGridPath="", instagramPath="";

	public static void main(String args[]) throws Exception {

		MPI.Init(args);
		int size = MPI.COMM_WORLD.Size();
		int me = MPI.COMM_WORLD.Rank();
		System.out.println(me + ": " + size);
		if (me==0) {
			if (args.length > 1){
				melbGridPath = args[3];
				instagramPath = args[4];
			} else {
				throw new FileNotFoundException();
			}
			master();
		}
		else {
			slave();
		}

		MPI.Finalize();
	}

	public static void master () {
		try {

			String[] work = new String[1];
			double[] result = new double[2];
			Status sta = new Status();
			int ntasks = MPI.COMM_WORLD.Size();
			String temp = "";
			String[] tempArr;
			BufferedReader br = new BufferedReader(new FileReader(instagramPath));

			//initialise Melbourne grid map
			MelbGrid mg = new MelbGrid();
	        mg.readStream(melbGridPath);
			mg.assignGridMap();

			work[0] = br.readLine();
			if (ntasks>1) {
				//initilise works
				for (int rank=1; rank<ntasks && work[0]!=null; rank++) {
					if (work[0].contains("coordinates")) {
						temp = work[0].substring(work[0].indexOf("coordinates"));
						if (temp.indexOf("[")+1<temp.length() && temp.indexOf("[")>=0 && temp.indexOf("]")>temp.indexOf("[")+1) {
							temp = temp.substring(temp.indexOf("[")+1, temp.indexOf("]")).replaceAll(" ", "").replaceAll("null", "");
							work[0] = temp;
							MPI.COMM_WORLD.Send(work, 0, 1, MPI.OBJECT, rank, contTag);
						}
					}
					else
						rank--;
					work[0] = br.readLine();
				}
				//receive and send works
				while (work[0]!=null) {
					if (work[0].contains("coordinates")) {
						temp = work[0].substring(work[0].indexOf("coordinates"));
						if (temp.indexOf("[")+1<temp.length() && temp.indexOf("[")>=0 && temp.indexOf("]")>temp.indexOf("[")+1) {
							temp = temp.substring(temp.indexOf("[")+1, temp.indexOf("]")).replaceAll(" ", "").replaceAll("null", "");
							work[0] = temp;
							sta = MPI.COMM_WORLD.Recv(result, 0, 2, MPI.DOUBLE, MPI.ANY_SOURCE, MPI.ANY_TAG);
							MPI.COMM_WORLD.Send(work, 0, 1, MPI.OBJECT, sta.source, contTag);
							if (result[0]!=notFound)
								mg.addPostCountToBox(createPoint(result));
						}
					}
					work[0] = br.readLine();
				}
				System.out.println(ntasks);
				for (int rank=1; rank<ntasks; rank++) {
					MPI.COMM_WORLD.Recv(result, 0, 2, MPI.DOUBLE, MPI.ANY_SOURCE, MPI.ANY_TAG);
					if (result[0]!=notFound)
						mg.addPostCountToBox(createPoint(result));
				}

				//exit after all works are done
				work[0] = null;
				for (int rank=1; rank<ntasks; rank++) {
					MPI.COMM_WORLD.Send(work, 0, 1, MPI.OBJECT, rank, exitTag);
				}
			} else {
				/*
				while (work[0]!=null) {
					if (work[0].contains("coordinates")) {
						temp = work[0].substring(work[0].indexOf("coordinates"));
						if (temp.indexOf("[")+1<temp.length() && temp.indexOf("[")>=0 && temp.indexOf("]")>temp.indexOf("[")+1) {
							temp = temp.substring(temp.indexOf("[")+1, temp.indexOf("]")).replaceAll(" ", "").replaceAll("null", "");
							tempArr = temp.split(",");

							if (tempArr.length==2) {
								result[0] = Double.valueOf(tempArr[0]);
								result[1] = Double.valueOf(tempArr[1]);
							} else {
								result[0] = notFound;
								result[1] = notFound;
							}

							if (result[0]!=notFound)
								mg.addPostCountToBox(createPoint(result));
						}
					}
					work[0] = br.readLine();
				}
				*/

			}
			br.close();

			//calculate Melbourne map, sort, and print results
			mg.mainProcessGrid();

		} catch (Exception e){
			System.out.println(e);
		}
	}
	public static void slave () {
		double[] result = new double[2];
		String[] work = new String[1];
		String temp = "";
		String[] tempArr;
		Status sta = new Status();
		while (true) {
			sta = MPI.COMM_WORLD.Recv(work, 0, 1, MPI.OBJECT, 0, MPI.ANY_TAG);
			if (sta.tag == exitTag) {
				break;
			}
			tempArr = work[0].split(",");

			if (tempArr.length==2) {
				result[0] = Double.valueOf(tempArr[0]);
				result[1] = Double.valueOf(tempArr[1]);
			} else {
				result[0] = notFound;
				result[1] = notFound;
			}
			MPI.COMM_WORLD.Send(result, 0, 2, MPI.DOUBLE, 0, contTag);
		}
	}
	public static Point createPoint (double[] d) {
		return new Point(d[0],d[1]);
	}
}
class MelbGrid {

	private static int numRow = 4, numCol = 5;
	private Box[] boxArr;
	private Row[] rowArr;
	private Column[] colArr;

	public MelbGrid () {
		rowArr = new Row[numRow];
		colArr = new Column[numCol];
	}

	public Box getBox(int index) {
		return boxArr[index];
	}

	public Row getRow(int index) {
		return rowArr[index];
	}

	public Column getCol(int index) {
		return colArr[index];
	}

	public void mainProcessGrid () {
    	addPostCountToGrid();
    	sortGrid();

    	System.out.println("--- Box ---");
    	for (int i=0; i<boxArr.length; i++) {
    		System.out.println(getBox(i).getId() + ": " + getBox(i).getPostCount() + " posts");
    	}
    	System.out.println("--- Row ---");
    	for (int i=0; i<rowArr.length; i++) {
    		System.out.println(getRow(i).getId() + "-Row " + ": " + getRow(i).getValue() + " posts");
    	}
    	System.out.println("--- Column ---");
    	for (int i=0; i<colArr.length; i++) {
    		System.out.println("Column " + getCol(i).getId() + ": " + getCol(i).getValue() + " posts");
    	}
	}

	public void readStream(String path) {
        try {

        	BufferedReader br = new BufferedReader(new FileReader(path));

			String line = br.readLine();

        	List <Box>boxes = new ArrayList<>();
        	String[] strArr, strArr2;
        	String id = "";
        	double xmin=0, xmax=0, ymin=0, ymax=0;
            while (line!=null) {
            	if (line.contains("id")) {
    				line = line.substring(line.indexOf("id"), line.indexOf("}"));
    				strArr = line.split(",");
    				for (int i=0; i<strArr.length; i++) {
    					strArr2=strArr[i].split(":");
    					switch (i) {
    						case 0: id = strArr2[1].trim().substring(1,3);
    							break;
    						case 1: xmin = Double.parseDouble(strArr2[1].trim());
								break;
    						case 2: xmax = Double.parseDouble(strArr2[1].trim());
								break;
    						case 3: ymin = Double.parseDouble(strArr2[1].trim());
								break;
    						case 4: ymax = Double.parseDouble(strArr2[1].trim());
								break;
    						default: break;
    					}

    				}
    				boxes.add(new Box(id, xmin, xmax, ymin, ymax));
    			}
            	line = br.readLine();
            }
            br.close();
            boxArr = new Box[boxes.size()];
            for (int i=0; i<boxArr.length; i++) {
            	boxArr[i] = boxes.get(i);
            }
        } catch (Exception ex) {
        	System.out.println(ex);
        }
    }
	public int isInBox (Point p) {
		if (p.getBoxId()==null) {
			for (int i=0; i<boxArr.length; i++) {
				if (p.getX()>=boxArr[i].getXmin() && p.getX()<=boxArr[i].getXmax() && p.getY()>=boxArr[i].getYmin() && p.getY()<=boxArr[i].getYmax()) {
					p.assignBoxId(boxArr[i].getId());
					return i;
				}
			}
		}
		return -1;
	}
	public void addPostCountToBox (Point p) {
		int index = isInBox(p);
		if (index>=0) {
			boxArr[index].addPostCount();
		}
	}
	public void addPostCountToGrid () {
		int[] rows = new int[numRow];
		int[] columns = new int[numCol];
		int r, c, count;
		for (int i=0; i<boxArr.length; i++) {
			r = boxArr[i].getRowNum();
			c = boxArr[i].getColNum();
			count = boxArr[i].getPostCount();
			rows[r] = rows[r] + count;
			columns[c] = columns[c] + count;
		}
		for (int i=0; i<numRow; i++) {
			rowArr[i] = new Row(i, rows[i]);
		}
		for (int i=0; i<numCol; i++) {
			colArr[i] = new Column(i, columns[i]);
		}
	}
	public void assignGridMap () {
		for (int i=0; i<boxArr.length; i++)
			boxArr[i].assignGrid();
	}
	public void sortBoxes() {
		for (int i=1; i<boxArr.length; i++) {
			for (int j=i; j>0; j--) {
				if (boxArr[j-1].getPostCount() < boxArr[j].getPostCount()) {
					Box temp = boxArr[j];
					boxArr[j] = boxArr[j-1];
					boxArr[j-1] = temp;
				}
			}
		}
	}
	public void sortRows() {
		for (int i=1; i<rowArr.length; i++) {
			for (int j=i; j>0; j--) {
				if (rowArr[j-1].getValue() < rowArr[j].getValue()) {
					Row temp = rowArr[j];
					rowArr[j] = rowArr[j-1];
					rowArr[j-1] = temp;
				}
			}
		}
	}
	public void sortColumns() {
		for (int i=1; i<colArr.length; i++) {
			for (int j=i; j>0; j--) {
				if (colArr[j-1].getValue() < colArr[j].getValue()) {
					Column temp = colArr[j];
					colArr[j] = colArr[j-1];
					colArr[j-1] = temp;
				}
			}
		}
	}
	public void sortGrid() {
		this.sortBoxes();
		this.sortRows();
		this.sortColumns();
	}
}

class Box {
	private String id;
	private double xmin, xmax, ymin, ymax;
	private int postCount, rowNum, colNum;

	public Box (String i, double x1, double x2, double y1, double y2) {
		id = i;
		xmin = x1;
		xmax = x2;
		ymin = y1;
		ymax = y2;
	}

	public String getId () {
		return id;
	}
	public double getXmin () {
		return xmin;
	}
	public double getXmax () {
		return xmax;
	}
	public double getYmin () {
		return ymin;
	}
	public double getYmax () {
		return ymax;
	}
	public int getPostCount () {
		return postCount;
	}
	public int getRowNum () {
		return rowNum;
	}
	public int getColNum () {
		return colNum;
	}
	public void addPostCount () {
		postCount = postCount+1;
	}
	public void assignRowNum () {
		char c = id.charAt(0);
		switch (c) {
			case 'A': rowNum = 0;
				break;
			case 'B': rowNum = 1;
				break;
			case 'C': rowNum = 2;
				break;
			case 'D': rowNum = 3;
				break;
			default: rowNum = -1;
				System.out.println("Row Error: Wrong box ID!");
				break;
		}
	}
	public void assignColNum () {
		char c = id.charAt(1);
		switch (c) {
		case '1': colNum = 0;
			break;
		case '2': colNum = 1;
			break;
		case '3': colNum = 2;
			break;
		case '4': colNum = 3;
			break;
		case '5': colNum = 4;
			break;
		default: colNum = -1;
			System.out.println("Column Error: Wrong box ID!");
			break;
		}
	}
	public void assignGrid () {
		this.assignRowNum();
		this.assignColNum();
	}
}

class Row {
	private String id;
	private int value;
	public Row (int i, int v) {
		value = v;
		switch (i) {
			case 0: id = "A";
				break;
			case 1: id = "B";
				break;
			case 2: id = "C";
				break;
			case 3: id = "D";
				break;
			default: id = null;
				break;
		}
	}
	public String getId () {
		return id;
	}
	public int getValue () {
		return value;
	}
}
class Column {
	private int id, value;
	public Column (int i, int v) {
		value = v;
		id = i+1;
	}
	public int getId () {
		return id;
	}
	public int getValue () {
		return value;
	}
}

class Point {
	double[] coordinates;
	String boxId;

	public Point () {
		coordinates = new double[2];
		boxId = null;
	}
	public Point (double y, double x) {
		coordinates = new double[2];
		boxId = null;
		coordinates[0] = y;
		coordinates[1] = x;
	}
	public double getX () {
		return coordinates[1];
	}
	public double getY () {
		return coordinates[0];
	}
	public String getBoxId () {
		return boxId;
	}
	public void assignBoxId (String s) {
		boxId = s;
	}
}
