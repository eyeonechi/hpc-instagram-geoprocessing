import java.io.Serializable;

public class Grid implements Serializable {
  String name;
  String column;
  String row;
  Float xmin;
  Float xmax;
  Float ymin;
  Float ymax;

  public String toString() {
    return this.name
      + "["
      + Float.toString(this.xmin)
      + Float.toString(this.xmax)
      + Float.toString(this.ymin)
      + Float.toString(this.ymax)
      + "]";
  }

  public Boolean inGrid(Float x, Float y) {
    return(x >= xmin && x <= xmax && y >= ymin && y <= ymax);
  }

}
